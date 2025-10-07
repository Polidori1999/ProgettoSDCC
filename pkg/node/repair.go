package node

import (
	"ProgettoSDCC/pkg/proto"
	"log"
	"time"
)

// faccio un giro di repair push–pull.
// 1) scelgo k peer (k≈log₂(n)) tra gli "alive";
// 2) PUSH: invio un HB(full) con epoch/ver/servizi/peers (serve epoch+ver per superare la guardia);
// 3) PULL: chiedo indietro il loro snapshot con una RepairReq.
func (n *Node) repairRound() {
	if !n.learnFromHB {
		return
	}
	peers := n.alivePeers()
	if len(peers) == 0 {
		return // niente da fare
	}
	k := logFanout(len(peers))
	targets := randomSubset(peers, k, n.GossipM.rnd)

	// HB(full) da pushare: DEVE includere Epoch/SvcVer per essere accettato dai peer
	full := proto.Heartbeat{
		Epoch:    n.Registry.LocalEpoch(),
		SvcVer:   n.Registry.LocalVersion(),
		Services: n.Registry.LocalServices(),
		Peers:    n.PeerMgr.List(),
	}
	pktFull, _ := proto.Encode(proto.MsgHeartbeat, n.ID, full)

	// Richiesta di repair (pull)
	req := proto.RepairReq{Nonce: time.Now().UnixNano()}
	pktReq, _ := proto.Encode(proto.MsgRepairReq, n.ID, req)

	for _, p := range targets {
		n.GossipM.SendUDP(pktFull, p) // PUSH
		n.GossipM.SendUDP(pktReq, p)  // PULL
	}
	log.Printf("[REPAIR] push-pull tick: k=%d targets=%v", k, targets)
}
