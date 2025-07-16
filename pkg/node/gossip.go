package node

import (
	"net"
	"time"
)

type GossipManager struct {
	peers  *PeerManager
	digest *DigestManager
	reg    *ServiceRegistry
	self   string
}

func NewGossipManager(pm *PeerManager, dm *DigestManager, reg *ServiceRegistry, selfID string) *GossipManager {
	return &GossipManager{
		peers:  pm,
		digest: dm,
		reg:    reg,
		self:   selfID,
	}
}

func (gm *GossipManager) Start() {
	ticker := time.NewTicker(3 * time.Second)
	go func() {
		for range ticker.C {
			d := gm.digest.Compute(gm.reg)
			for _, peer := range gm.peers.List() {
				if gm.digest.Changed(peer, d) {
					msg := MakeHeartbeatWithDigest(gm.reg, gm.self, d, gm.peers.List())

					gm.SendUDP(msg, peer)
				}
			}
		}
	}()
}

func (gm *GossipManager) SendUDP(data []byte, peerAddr string) {
	addr, err := net.ResolveUDPAddr("udp", peerAddr)
	if err != nil {
		return
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return
	}
	conn.Write(data)
	conn.Close()
}
