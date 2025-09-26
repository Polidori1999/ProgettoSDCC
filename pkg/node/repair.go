package node

import (
	"ProgettoSDCC/pkg/proto"
	"log"
	"math"
	"time"
)

// Esegue un giro di repair: PUSH il mio snapshot, poi PULL il loro.
func (n *Node) repairRound() {
	peers := n.alivePeers()
	if len(peers) == 0 {
		return
	}
	k := int(math.Ceil(math.Log2(float64(len(peers)))))
	if k < 1 {
		k = 1
	}
	targets := randomSubset(peers, k, n.GossipM.rnd)

	full := proto.Heartbeat{
		Services: n.Registry.LocalServices(),
		Peers:    n.PeerMgr.List(),
	}
	pktFull, _ := proto.Encode(proto.MsgHeartbeat, n.ID, full)

	req := proto.RepairReq{Nonce: time.Now().UnixNano()}
	pktReq, _ := proto.Encode(proto.MsgRepairReq, n.ID, req)

	for _, p := range targets {
		n.GossipM.SendUDP(pktFull, p)
		n.GossipM.SendUDP(pktReq, p)
	}
	log.Printf("[REPAIR] push-pull tick: k=%d targets=%v", k, targets)
}
