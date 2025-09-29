package node

import (
	"ProgettoSDCC/pkg/proto"
	"math/rand"
	"net"
	"time"
)

type GossipManager struct {
	peers *PeerManager

	reg  *ServiceRegistry
	self string

	// rnd è il generatore casuale locale
	rnd *rand.Rand
	// NEW
	stopCh     chan struct{}
	lightT     *time.Ticker
	fullT      *time.Ticker
	hintIdx    int
	lightEvery time.Duration
	fullEvery  time.Duration
}

// NewGossipManager ora riceve un generatore *rand.Rand
func NewGossipManager(pm *PeerManager, reg *ServiceRegistry, selfID string, r *rand.Rand) *GossipManager {
	return &GossipManager{
		peers: pm,
		reg:   reg,
		self:  selfID,
		rnd:   r,
	}
}

func (gm *GossipManager) Start() {
	if gm.stopCh != nil {
		return // già avviato
	}
	// Niente default hardcoded: devono essere impostati via SetHeartbeatIntervals
	if gm.lightEvery <= 0 || gm.fullEvery <= 0 {
		panic("GossipManager: heartbeat intervals not set (call SetHeartbeatIntervals before Start)")
	}

	gm.stopCh = make(chan struct{})
	gm.lightT = time.NewTicker(gm.lightEvery)
	gm.fullT = time.NewTicker(gm.fullEvery)

	go func(stop <-chan struct{}) {
		defer func() {
			if gm.lightT != nil {
				gm.lightT.Stop()
			}
			if gm.fullT != nil {
				gm.fullT.Stop()
			}
		}()
		for {
			select {
			case <-stop:
				return
			case <-gm.lightT.C:
				gm.sendLightHB()
			case <-gm.fullT.C:
				gm.sendFullHB()
			}
		}
	}(gm.stopCh)
}

func (gm *GossipManager) sendLightHB() {
	hb := proto.Heartbeat{
		Epoch:  gm.reg.LocalEpoch(),
		SvcVer: gm.reg.LocalVersion(),
		Peers:  gm.peerHints(2),
	}
	pkt, _ := proto.Encode(proto.MsgHeartbeatLight, gm.self, hb)
	gm.fanout(pkt)
}

func (gm *GossipManager) sendFullHB() {
	hb := proto.Heartbeat{
		Epoch:    gm.reg.LocalEpoch(),
		SvcVer:   gm.reg.LocalVersion(),
		Services: gm.reg.LocalServices(),
		Peers:    gm.peers.List(),
	}
	pkt, _ := proto.Encode(proto.MsgHeartbeat, gm.self, hb)
	gm.fanout(pkt)
}

func (gm *GossipManager) Stop() {
	if gm.stopCh != nil {
		close(gm.stopCh)
		gm.stopCh = nil
	}
	if gm.lightT != nil {
		gm.lightT.Stop()
		gm.lightT = nil
	}
	if gm.fullT != nil {
		gm.fullT.Stop()
		gm.fullT = nil
	}
}

func (gm *GossipManager) fanout(pkt []byte) {
	peers := gm.peers.List()
	n := len(peers)
	if n == 0 {
		return
	}
	k := logFanout(n)

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
func (gm *GossipManager) TriggerHeartbeatFullNow() {

	gm.sendFullHB()
}

func (gm *GossipManager) peerHints(max int) []string {
	all := gm.peers.List()
	n := len(all)
	if n == 0 || max <= 0 {
		return nil
	}
	if n <= max {
		return all
	}
	start := gm.hintIdx % n
	gm.hintIdx = (gm.hintIdx + max) % n
	out := make([]string, 0, max)
	for i := 0; i < max; i++ {
		out = append(out, all[(start+i)%n])
	}
	return out
}

func (g *GossipManager) SetHeartbeatIntervals(light, full time.Duration) {
	if light > 0 {
		g.lightEvery = light
	}
	if full > 0 {
		g.fullEvery = full
	}
}
