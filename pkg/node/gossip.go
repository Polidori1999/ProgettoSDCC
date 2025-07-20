package node

import (
	"math"
	"math/rand"
	"net"
	"time"
)

type GossipManager struct {
	peers  *PeerManager
	digest *DigestManager
	reg    *ServiceRegistry
	self   string

	// rnd è il generatore casuale locale
	rnd *rand.Rand
}

// NewGossipManager ora riceve un generatore *rand.Rand
func NewGossipManager(pm *PeerManager, dm *DigestManager, reg *ServiceRegistry, selfID string, r *rand.Rand) *GossipManager {
	return &GossipManager{
		peers:  pm,
		digest: dm,
		reg:    reg,
		self:   selfID,
		rnd:    r,
	}
}

func (gm *GossipManager) Start() {
	// round di gossip ogni 1 secondo
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			peers := gm.peers.List()
			n := len(peers)
			if n == 0 {
				continue
			}

			// digest corrente dello stato servizi
			digest := gm.digest.Compute(gm.reg)

			// k = ceil(log2(n)), almeno 1 ma non oltre n
			k := int(math.Ceil(math.Log2(float64(n))))
			if k < 1 {
				k = 1
			}
			if k > n {
				k = n
			}

			// mescola i peer casualmente
			gm.rnd.Shuffle(n, func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })

			// invia solo ai primi k peer
			for _, peer := range peers[:k] {
				msg := MakeHeartbeatWithDigest(gm.reg, gm.self, digest, gm.peers.List())
				gm.SendUDP(msg, peer)
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
