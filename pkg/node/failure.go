package node

import (
	"ProgettoSDCC/pkg/proto"
	"fmt"
	"log"
	"sync"
	"time"
)

type FailureDetector struct {
	mu             sync.Mutex
	peers          *PeerManager
	reg            *ServiceRegistry
	gossip         *GossipManager
	suspectTimeout time.Duration
	failTimeout    time.Duration

	suspected  map[string]bool // peer → già gossipato SuspectRumor
	suppressed map[string]bool // peer → già gossipato DeadRumor / rimosso

	// NEW: canale di stop per chiudere la goroutine pulitamente
	stopCh  chan struct{}
	fanoutB int
	maxFwF  int
	ttlT    int
}

func (fd *FailureDetector) SetGossipParams(B, F, T int) {
	if B < 1 {
		B = 1
	}
	if B > 255 {
		B = 255
	}
	if F < 1 {
		F = 1
	}
	if F > 255 {
		F = 255
	}
	if T < 1 {
		T = 1
	}
	if T > 255 {
		T = 255
	}
	fd.fanoutB, fd.maxFwF, fd.ttlT = B, F, T
}

func NewFailureDetector(pm *PeerManager, reg *ServiceRegistry, gm *GossipManager, suspectT, failT time.Duration) *FailureDetector {
	return &FailureDetector{
		peers:          pm,
		reg:            reg,
		gossip:         gm,
		suspectTimeout: suspectT,
		failTimeout:    failT,
		suspected:      make(map[string]bool),
		suppressed:     make(map[string]bool),
		// stopCh viene creato in Start() per permettere eventuali ri-avvii
	}
}

// ferma ogni futura segnalazione per peer
func (fd *FailureDetector) SuppressPeer(peer string) {
	fd.mu.Lock()
	fd.suppressed[peer] = true
	delete(fd.suspected, peer)
	fd.mu.Unlock()
}

// riabilita peer (es. dopo un leave + heartbeat)
func (fd *FailureDetector) UnsuppressPeer(peer string) {
	fd.mu.Lock()
	delete(fd.suppressed, peer)
	delete(fd.suspected, peer)
	fd.mu.Unlock()
	// “Finge” di aver appena visto un heartbeat
	fd.peers.Seen(peer)
}

// S avvia il monitoraggio dei peer, gossippando SuspectRumor.
// NEW: versione con select su ticker e stopCh.
func (fd *FailureDetector) Start() {
	fd.mu.Lock()
	if fd.stopCh != nil {
		// già in esecuzione
		fd.mu.Unlock()
		return
	}
	ch := make(chan struct{})
	fd.stopCh = ch
	fd.mu.Unlock()

	ticker := time.NewTicker(1 * time.Second)

	go func(stop chan struct{}) {
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				snap := fd.peers.SnapshotLastSeen() // <<< copia consistente
				for peer, last := range snap {
					fd.mu.Lock()
					if fd.suppressed[peer] {
						fd.mu.Unlock()
						continue
					}
					fd.mu.Unlock()

					age := now.Sub(last)

					// 1) se è rientrato (< suspectTimeout), resettiamo lo stato
					if age <= fd.suspectTimeout {
						fd.mu.Lock()
						if fd.suspected[peer] {
							delete(fd.suspected, peer)
							log.Printf("Peer %s OK (rientrato dal SUSPECT)", peer) // facoltativo ma utile
						}
						fd.mu.Unlock()
						continue
					}

					// 2) fase di SUSPECT
					if age > fd.suspectTimeout && age <= fd.failTimeout {
						fd.mu.Lock()
						first := !fd.suspected[peer]
						if first {
							fd.suspected[peer] = true
						}
						fd.mu.Unlock()

						if first {
							log.Printf("Peer %s SUSPICIOUS (no heartbeat for %v)", peer, age)

							rumorID := fmt.Sprintf("suspect|%s|%d", peer, now.UnixNano())
							sr := proto.SuspectRumor{
								RumorID: rumorID,
								Peer:    peer,
								Fanout:  uint8(fd.fanoutB), // B
								MaxFw:   uint8(fd.maxFwF),  // F
								TTL:     uint8(fd.ttlT),    // T
							}
							out, err := proto.Encode(proto.MsgSuspect, fd.gossip.self, sr)
							if err == nil {
								// targets = tutti i peer tranne il sospetto
								all := fd.peers.List()
								filtered := make([]string, 0, len(all))
								for _, p := range all {
									if p != peer {
										filtered = append(filtered, p)
									}
								}

								// fanout B fisso (gossip), non k=log N
								B := fd.fanoutB
								if B < 1 {
									B = 1
								}
								if B > len(filtered) {
									B = len(filtered)
								}

								for _, p := range randomSubset(filtered, B, fd.gossip.rnd) {
									fd.gossip.SendUDP(out, p)
								}
								log.Printf("[FD] SUSPECT GOSSIP %s B=%d F=%d TTL=%d targets=%v",
									peer, fd.fanoutB, fd.maxFwF, fd.ttlT, filtered)

								// self-vote via loopback (fa scattare subito il tuo handler MsgSuspect)
								fd.gossip.SendUDP(out, fd.gossip.self)
							}
							continue
						}

						continue
					}
				}

			case <-stop:
				// richiesta di stop
				return
			}
		}
	}(ch)
}

// NEW:  ferma la goroutine del failure detector
func (fd *FailureDetector) Stop() {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	if fd.stopCh != nil {
		close(fd.stopCh)
		fd.stopCh = nil
	}
}
