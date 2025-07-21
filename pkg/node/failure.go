package node

import (
	"fmt"
	"log"
	"sync"
	"time"

	"ProgettoSDCC/pkg/proto"
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
			for _, peer := range fd.peers.List() {
				fd.mu.Lock()
				// se ho già dichiarato dead, skippo del tutto
				if fd.suppressed[peer] {
					fd.mu.Unlock()
					continue
				}
				last, ok := fd.peers.LastSeen[peer]
				fd.mu.Unlock()
				if !ok {
					continue
				}

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
							for _, p := range append(fd.peers.List(), fd.gossip.self) {
								fd.gossip.SendUDP(out, p)
							}
						}
					}
					continue
				}

				// 3) fase di DEAD
				if age > fd.failTimeout {
					fd.mu.Lock()
					already := fd.suppressed[peer]
					if !already {
						fd.suppressed[peer] = true
					}
					fd.mu.Unlock()

					if !already {
						log.Printf("Peer %s DEAD (no heartbeat for %v)", peer, age)
						rumorID := fmt.Sprintf("dead|%s|%d", peer, now.UnixNano())
						dr := proto.DeadRumor{RumorID: rumorID, Peer: peer}
						out, err := proto.Encode(proto.MsgDead, fd.gossip.self, dr)
						if err == nil {
							for _, p := range append(fd.peers.List(), fd.gossip.self) {
								fd.gossip.SendUDP(out, p)
							}
						}
						// rimozione locale
						fd.peers.Remove(peer)
						fd.reg.RemoveProvider(peer)
					}
					continue
				}
			}
		}
	}()
}
