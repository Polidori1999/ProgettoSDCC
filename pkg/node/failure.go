package node

import (
	"log"
	"time"
)

type FailureDetector struct {
	peers   *PeerManager
	reg     *ServiceRegistry
	timeout time.Duration
}

func NewFailureDetector(pm *PeerManager, reg *ServiceRegistry, timeout time.Duration) *FailureDetector {
	return &FailureDetector{
		peers:   pm,
		reg:     reg,
		timeout: timeout,
	}
}

func (fd *FailureDetector) Start() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			now := time.Now()
			for _, peer := range fd.peers.List() {
				last := fd.peers.LastSeen[peer]
				if now.Sub(last) > fd.timeout {
					fd.peers.Remove(peer)
					fd.reg.RemoveProvider(peer)
					log.Printf("Peer %s DEAD", peer)
				}
			}
		}
	}()
}
