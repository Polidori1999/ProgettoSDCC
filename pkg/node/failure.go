package node

import (
	"fmt"
	"log"
	"time"

	"ProgettoSDCC/pkg/proto"
)

type FailureDetector struct {
	peers          *PeerManager
	reg            *ServiceRegistry
	gossip         *GossipManager
	suspectTimeout time.Duration
	failTimeout    time.Duration
	suspected      map[string]bool // peer → già gossipato SuspectRumor
}

// NewFailureDetector crea un failure detector con due soglie: suspectTimeout e failTimeout.
func NewFailureDetector(pm *PeerManager, reg *ServiceRegistry, gm *GossipManager, suspectT, failT time.Duration) *FailureDetector {
	return &FailureDetector{
		peers:          pm,
		reg:            reg,
		gossip:         gm,
		suspectTimeout: suspectT,
		failTimeout:    failT,
		suspected:      make(map[string]bool),
	}
}

// Start avvia il monitoraggio dei peer, gossippando SuspectRumor e DeadRumor.
func (fd *FailureDetector) Start() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for now := range ticker.C {
			for _, peer := range fd.peers.List() {
				last := fd.peers.LastSeen[peer]
				age := now.Sub(last)

				// se il peer è tornato in vita (< suspectTimeout), resettiamo il flag
				if age <= fd.suspectTimeout {
					if fd.suspected[peer] {
						delete(fd.suspected, peer)
					}
					continue
				}

				// Fase di sospetto
				if age > fd.suspectTimeout && age <= fd.failTimeout {
					if !fd.suspected[peer] {
						fd.suspected[peer] = true
						log.Printf("Peer %s SUSPICIOUS (no heartbeat for %v)", peer, age)

						// Genera SuspectRumor
						rumorID := fmt.Sprintf("suspect|%s|%d", peer, now.UnixNano())
						sr := proto.SuspectRumor{RumorID: rumorID, Peer: peer}
						out, err := proto.Encode(proto.MsgSuspect, fd.gossip.self, sr) // ❶ From = self
						if err == nil {
							targets := append(fd.peers.List(), fd.gossip.self) // ❷ invia anche a sé
							for _, p := range targets {
								fd.gossip.SendUDP(out, p)
							}
						}
					}
					continue
				}

				if age > fd.failTimeout {
					continue
				}
			}
		}
	}()
}
