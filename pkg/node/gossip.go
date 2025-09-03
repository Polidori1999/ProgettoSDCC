package node

import (
	"ProgettoSDCC/pkg/proto"
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

	lightTick := time.NewTicker(2 * time.Second) // HB “digest”
	fullTick := time.NewTicker(15 * time.Second) // HB completo

	go func() {
		for {
			select {
			case <-lightTick.C:
				gm.sendLightHB()
			case <-fullTick.C:
				gm.sendFullHB()
			}
		}
	}()
	//da inserire
	/*go func() {
	    for {
	        time.Sleep(2*time.Second + time.Duration(gm.rnd.Intn(400)-200)*time.Millisecond)
	        gm.sendLightHB()
	    }
	}()
	go func() {
	    for {
	        time.Sleep(15*time.Second + time.Duration(gm.rnd.Intn(1500)-750)*time.Millisecond)
	        gm.sendFullHB()
	    }
	}()*/
}

func (gm *GossipManager) sendLightHB() {
	hb := proto.Heartbeat{
		Digest: gm.digest.Compute(gm.reg),
		Peers:  gm.peers.List(), // se vuoi il piggy-back peer
	}
	pkt, _ := proto.Encode(proto.MsgHeartbeatDigest, gm.self, hb)
	gm.fanout(pkt)
}

func (gm *GossipManager) sendFullHB() {
	hb := proto.Heartbeat{
		Services: gm.reg.LocalServices(), // solo i MIEI servizi
		Digest:   gm.digest.Compute(gm.reg),
		Peers:    gm.peers.List(),
	}
	pkt, _ := proto.Encode(proto.MsgHeartbeat, gm.self, hb)
	gm.fanout(pkt)
}

func (gm *GossipManager) fanout(pkt []byte) {
	peers := gm.peers.List()
	n := len(peers)
	if n == 0 {
		return
	}
	k := int(math.Ceil(math.Log2(float64(n))))
	if k < 1 {
		k = 1
	}
	if k > n {
		k = n
	}

	gm.rnd.Shuffle(n, func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	for _, p := range peers[:k] {
		gm.SendUDP(pkt, p)
	}
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
