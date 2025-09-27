package node

import (
	"ProgettoSDCC/pkg/proto"
	"fmt"
	"log"
	"sync"
	"time"
)

// Parametri gossip (B,F,T) incapsulati
type RumorParams struct {
	Fanout      int // B
	MaxForwards int // F
	TTL         int // T
}

type FailureDetector struct {
	mu             sync.Mutex
	peers          *PeerManager
	reg            *ServiceRegistry // può restare anche se non usata
	gossip         *GossipManager
	suspectTimeout time.Duration
	failTimeout    time.Duration

	suspected  map[string]bool // peer → già gossipato SuspectRumor
	suppressed map[string]bool // peer → già gossipato DeadRumor / rimosso

	stopCh  chan struct{}
	fanoutB int
	maxFwF  int
	ttlT    int
}

func clampByteRange(x int) int {
	if x < 1 {
		return 1
	}
	if x > 255 {
		return 255
	}
	return x
}

func (fd *FailureDetector) SetRumorParams(p RumorParams) {
	fd.mu.Lock()
	fd.fanoutB = clampByteRange(p.Fanout)
	fd.maxFwF = clampByteRange(p.MaxForwards)
	fd.ttlT = clampByteRange(p.TTL)
	fd.mu.Unlock()
}

// Back-compat col codice esistente
func (fd *FailureDetector) SetGossipParams(B, F, T int) {
	fd.SetRumorParams(RumorParams{Fanout: B, MaxForwards: F, TTL: T})
}

func (fd *FailureDetector) Params() RumorParams {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	return RumorParams{fd.fanoutB, fd.maxFwF, fd.ttlT}
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

// helper: emetti un SuspectRumor con i parametri correnti
func (fd *FailureDetector) emitSuspect(peer string, now time.Time) {
	p := fd.Params()

	rumorID := fmt.Sprintf("suspect|%s|%d", peer, now.UnixNano())
	sr := proto.SuspectRumor{
		RumorID: rumorID,
		Peer:    peer,
		Fanout:  uint8(p.Fanout),
		MaxFw:   uint8(p.MaxForwards),
		TTL:     uint8(p.TTL),
	}
	out, err := proto.Encode(proto.MsgSuspect, fd.gossip.self, sr)
	if err != nil {
		return
	}

	// targets = tutti i peer tranne il sospetto
	all := fd.peers.List()
	filtered := make([]string, 0, len(all))
	for _, x := range all {
		if x != peer {
			filtered = append(filtered, x)
		}
	}

	B := p.Fanout
	if B < 1 {
		B = 1
	}
	if B > len(filtered) {
		B = len(filtered)
	}
	for _, to := range randomSubset(filtered, B, fd.gossip.rnd) {
		fd.gossip.SendUDP(out, to)
	}
	log.Printf("[FD] SUSPECT GOSSIP %s B=%d F=%d TTL=%d targets=%v", peer, p.Fanout, p.MaxForwards, p.TTL, filtered)

	// self-vote via loopback (fa scattare subito il tuo handler MsgSuspect nel Node)
	fd.gossip.SendUDP(out, fd.gossip.self)
}

// Avvia il monitoraggio dei peer, gossippando SuspectRumor.
func (fd *FailureDetector) Start() {
	fd.mu.Lock()
	if fd.stopCh != nil {
		fd.mu.Unlock()
		return // già in esecuzione
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
				snap := fd.peers.SnapshotLastSeen() // copia consistente
				for peer, last := range snap {
					// salta se soppresso
					fd.mu.Lock()
					supp := fd.suppressed[peer]
					fd.mu.Unlock()
					if supp {
						continue
					}

					age := now.Sub(last)

					// 1) rientrato (< suspectTimeout) → reset stato SUSPECT
					if age <= fd.suspectTimeout {
						fd.mu.Lock()
						if fd.suspected[peer] {
							delete(fd.suspected, peer)
							log.Printf("Peer %s OK (rientrato dal SUSPECT)", peer)
						}
						fd.mu.Unlock()
						continue
					}

					// 2) finestra SUSPECT: (suspectTimeout, failTimeout]
					if age > fd.suspectTimeout && age <= fd.failTimeout {
						fd.mu.Lock()
						first := !fd.suspected[peer]
						if first {
							fd.suspected[peer] = true
						}
						fd.mu.Unlock()

						if first {
							log.Printf("Peer %s SUSPICIOUS (no heartbeat for %v)", peer, age)
							fd.emitSuspect(peer, now)
						}
						continue
					}

					// 3) oltre failTimeout → qui non facciamo nulla:
					// la conferma DEAD viene decisa via quorum nel Node.
				}

			case <-stop:
				return
			}
		}
	}(ch)
}

// Ferma la goroutine del failure detector
func (fd *FailureDetector) Stop() {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	if fd.stopCh != nil {
		close(fd.stopCh)
		fd.stopCh = nil
	}
}

// Imposta i timeouts (SUSPECT/FAIL) a runtime
func (fd *FailureDetector) SetTimeouts(suspect, fail time.Duration) {
	if suspect > 0 {
		fd.suspectTimeout = suspect
	}
	if fail > 0 {
		fd.failTimeout = fail
	}
}
