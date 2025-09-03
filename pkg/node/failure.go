package node

import (
	"ProgettoSDCC/pkg/proto"
	"fmt"
	"log"
	"math"
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
	}
}

// SuppressPeer ferma ogni futura segnalazione per peer
func (fd *FailureDetector) SuppressPeer(peer string) {
	fd.mu.Lock()
	fd.suppressed[peer] = true
	delete(fd.suspected, peer)
	fd.mu.Unlock()
}

// UnsuppressPeer riabilita peer (es. dopo un leave + heartbeat)
func (fd *FailureDetector) UnsuppressPeer(peer string) {
	fd.mu.Lock()
	delete(fd.suppressed, peer)
	delete(fd.suspected, peer)
	fd.mu.Unlock()
	// “Finge” di aver appena visto un heartbeat
	fd.peers.Seen(peer)
}

// Start avvia il monitoraggio dei peer, gossippando SuspectRumor e DeadRumor.
func (fd *FailureDetector) Start() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for now := range ticker.C {
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
						sr := proto.SuspectRumor{RumorID: rumorID, Peer: peer}
						out, err := proto.Encode(proto.MsgSuspect, fd.gossip.self, sr)
						if err == nil {
							// --- fanout calcolato sui TARGETS (escludendo il sospetto) ---
							all := fd.peers.List()
							n := len(all) // peers noti (può includere il sospetto)
							if n == 0 {
								log.Printf("[FD] suspect fanout: nessun peer noto (exclude=%s)", peer)
							} else {
								// filtra il sospetto
								filtered := make([]string, 0, n)
								for _, p := range all {
									if p != peer {
										filtered = append(filtered, p)
									}
								}
								m := len(filtered) // targets effettivi (senza il sospetto)
								if m > 0 {
									k := int(math.Ceil(math.Log2(float64(m))))
									if k < 1 {
										k = 1
									}
									if k > m {
										k = m
									}
									targets := randomSubset(filtered, k, fd.gossip.rnd)

									log.Printf("[FD] suspect fanout k=%d n=%d m=%d targets=%v exclude=%s",
										k, n, m, targets, peer)

									for _, p := range targets {
										fd.gossip.SendUDP(out, p)
									}
								} else {
									log.Printf("[FD] suspect fanout: nessun target (solo il sospetto era noto) exclude=%s", peer)
								}
							}

							// --- self-vote immediato via loopback ---
							// NB: peers.List() in genere NON contiene self, quindi lo inviamo esplicitamente a noi stessi
							fd.gossip.SendUDP(out, fd.gossip.self)
						}
					}

					continue
				}

			}
		}
	}()
}
