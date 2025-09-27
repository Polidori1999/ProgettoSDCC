package node

import (
	"ProgettoSDCC/pkg/proto"
	"fmt"
	"log"
	"strings"
	"time"
)

// =============================
//  Helper condiviso (revival)
// =============================

// gestisce la guardia tombstone: se il meta remoto (epoch,ver)
// è più nuovo del tombstone salvato, pulisce lo stato locale relativo al peer
// (dead/leave/suspect visti) e unsuppress sul FailureDetector.
// Ritorna true se si può applicare normalmente l’HB; false se va ignorato.
func (n *Node) applyRevivalIfNewer(peer string, epoch int64, ver uint64) bool {
	if eT, vT, hadTomb := n.Registry.TombMeta(peer); hadTomb {
		// ignora se NON più nuovo del tombstone
		if !(epoch > eT || (epoch == eT && ver > vT)) {
			return false
		}
		// revival: pulizia + unsuppress
		n.Registry.ClearTombMeta(peer)

		n.rumorMu.Lock()
		delete(n.handledDead, peer)
		delete(n.seenLeave, peer)
		n.suspectCount[peer] = 0
		for id := range n.seenSuspect {
			if strings.HasPrefix(id, "suspect|"+peer+"|") {
				delete(n.seenSuspect, id)
			}
		}
		for id := range n.deadSeenCnt {
			if strings.HasPrefix(id, "dead|"+peer+"|") {
				delete(n.deadSeenCnt, id)
			}
		}

		n.rumorMu.Unlock()

		n.FailureD.UnsuppressPeer(peer)
		log.Printf("Peer %s RI-ENTRATO (revival) epoch=%d ver=%d", peer, epoch, ver)
	}
	return true
}

// =============================
//  Handler: REPAIR request
// =============================

// invia indietro un Heartbeat full con la vista locale.
func (n *Node) handleRepairReq(env proto.Envelope) {
	if _, err := proto.DecodePayload[proto.RepairReq](env.Data); err != nil {
		// payload malformato: ignoro
		return
	}
	resp := proto.Heartbeat{
		Epoch:    n.Registry.LocalEpoch(),
		SvcVer:   n.Registry.LocalVersion(),
		Services: n.Registry.LocalServices(),
		Peers:    n.PeerMgr.List(),
	}
	out, _ := proto.Encode(proto.MsgHeartbeat, n.ID, resp)
	n.GossipM.SendUDP(out, env.From)
	log.Printf("[REPAIR] request from %s → sent HB(full) back", env.From)
}

// =============================
//  Handler: Heartbeat (light)
// =============================

func (n *Node) handleHeartbeatLight(env proto.Envelope, hbd proto.HeartbeatLight) {
	peer := env.From

	// guardia tombstone/riammissione
	if !n.applyRevivalIfNewer(peer, hbd.Epoch, hbd.SvcVer) {
		return
	}

	// piggyback dei peer (salta quelli tombstoned/left)
	for _, p2 := range hbd.Peers {
		if p2 == n.ID {
			continue
		}
		n.rumorMu.RLock()
		tombLocal := n.handledDead[p2] || n.seenLeave[p2]
		n.rumorMu.RUnlock()
		if tombLocal {
			continue
		}
		if _, _, tombReg := n.Registry.TombMeta(p2); tombReg {
			continue
		}
		n.PeerMgr.LearnFromPiggyback(p2)
	}

	// se il meta remoto è più nuovo, chiedi subito REPAIR
	if e0, v0, ok := n.Registry.RemoteMeta(peer); !ok || hbd.Epoch > e0 || (hbd.Epoch == e0 && hbd.SvcVer > v0) {
		req := proto.RepairReq{Nonce: time.Now().UnixNano()}
		out, _ := proto.Encode(proto.MsgRepairReq, n.ID, req)
		n.GossipM.SendUDP(out, peer)
	}

	// aggiorna vista locale
	n.PeerMgr.Add(peer)
	n.PeerMgr.Seen(peer)
	n.updateQuorum()
	log.Printf("HB(light) from %-12s peers=%v", peer, hbd.Peers)
}

// =============================
//  Handler: Heartbeat (full)
// =============================

func (n *Node) handleHeartbeatFull(env proto.Envelope, hb proto.Heartbeat) {
	peer := env.From

	// guardia tombstone/riammissione
	if !n.applyRevivalIfNewer(peer, hb.Epoch, hb.SvcVer) {
		return
	}

	// piggyback dei peer (salta tombstoned/left)
	for _, p2 := range hb.Peers {
		if p2 == n.ID {
			continue
		}
		n.rumorMu.RLock()
		tombLocal := n.handledDead[p2] || n.seenLeave[p2]
		n.rumorMu.RUnlock()
		if tombLocal {
			continue
		}
		if _, _, tombReg := n.Registry.TombMeta(p2); tombReg {
			continue
		}
		n.PeerMgr.LearnFromPiggyback(p2)
	}

	// update vista locale e servizi
	n.PeerMgr.Add(peer)
	n.PeerMgr.Seen(peer)
	n.updateQuorum()

	// normalizza e filtra servizi validi
	valid := make([]string, 0, len(hb.Services))
	for _, s := range hb.Services {
		s = strings.TrimSpace(strings.ToLower(s))
		if proto.IsValidService(s) {
			valid = append(valid, s)
		}
	}

	// aggiorna registry solo se epoch/ver sono nuovi
	if n.Registry.UpdateWithVersion(peer, valid, hb.Epoch, hb.SvcVer) {
		log.Printf("HB(full)   from %-12s epoch=%d ver=%d services=%v peers=%v", peer, hb.Epoch, hb.SvcVer, valid, hb.Peers)
	} else {
		log.Printf("HB(full)   from %-12s ignorato (epoch/ver non nuovi)", peer)
	}
}

// =============================
//  Handler: Lookup (deleghe)
// =============================

func (n *Node) handleLookup(env proto.Envelope, lm *LookupManager) {
	lm.HandleRequest(env)
}

func (n *Node) handleLookupResponse(env proto.Envelope, lm *LookupManager) {
	lm.HandleResponse(env)
}

// =============================
//  Handler: Suspect rumor
// =============================

func (n *Node) handleSuspect(env proto.Envelope, r proto.SuspectRumor) {
	// BLIND-COUNTER: incremento viste; stop oltre F
	n.rumorMu.Lock()
	cnt := n.suspectSeenCnt[r.RumorID] + 1
	n.suspectSeenCnt[r.RumorID] = cnt
	tomb := n.seenLeave[r.Peer] || n.handledDead[r.Peer]
	n.rumorMu.Unlock()
	if tomb {
		return
	}
	if r.MaxFw > 0 && cnt > r.MaxFw {
		return
	}

	// Prima vista? conta voto e verifica quorum → eventualmente emetti DEAD
	n.rumorMu.Lock()
	first := !n.seenSuspect[r.RumorID]
	if first {
		n.seenSuspect[r.RumorID] = true
		n.suspectCount[r.Peer]++
	}
	votes := n.suspectCount[r.Peer]
	need := n.quorumThreshold
	n.rumorMu.Unlock()

	if first && votes >= need && !n.handledDead[r.Peer] {
		n.rumorMu.Lock()
		n.handledDead[r.Peer] = true
		n.rumorMu.Unlock()
		log.Printf("Peer %s DEAD (quorum %d raggiunto)", r.Peer, need)

		// costruiamo DEAD rumor e inoltriamo a B peer vivi (escludi morto)
		tsNow := time.Now().UnixNano()
		d := proto.DeadRumor{
			RumorID: fmt.Sprintf("dead|%s|%d", r.Peer, tsNow),
			Peer:    r.Peer,
			Fanout:  uint8(n.fdB),
			MaxFw:   uint8(n.fdF),
			TTL:     uint8(n.fdT),
		}
		outD, _ := proto.Encode(proto.MsgDead, n.ID, d)

		targets := exclude(n.alivePeers(), r.Peer)
		B := n.fdB
		if B < 1 {
			B = 1
		}
		if B > len(targets) {
			B = len(targets)
		}
		for _, p := range randomSubset(targets, B, n.GossipM.rnd) {
			n.GossipM.SendUDP(outD, p)
		}

		// effetti locali di DEAD (con tombstone meta per guardia revival)
		if e, v, ok := n.Registry.RemoteMeta(r.Peer); ok {
			n.Registry.SaveTombMeta(r.Peer, e, v)
		}
		n.PeerMgr.Remove(r.Peer)
		n.FailureD.SuppressPeer(r.Peer)
		n.Registry.RemoveProvider(r.Peer)
		n.Registry.ResetRemoteMeta(r.Peer)
		n.updateQuorum()
		delete(n.suspectCount, r.Peer)
	}

	// Forward per-hop (GOSSIP) con TTL--
	if r.TTL > 0 {
		r.TTL--
		outS, _ := proto.Encode(proto.MsgSuspect, n.ID, r)
		targets := n.pickTargets(exclude(n.alivePeers(), env.From, r.Peer), int(r.Fanout))
		for _, p := range targets {
			n.GossipM.SendUDP(outS, p)
		}
		log.Printf("fwd SUSPECT %s TTL=%d B=%d seen=%d/%d", r.Peer, r.TTL, len(targets), cnt, r.MaxFw)
	}

}

// =============================
//  Handler: Dead rumor
// =============================

func (n *Node) handleDead(env proto.Envelope, d proto.DeadRumor) {
	// Filtro freschezza: se per me il peer è “fresco”, non applico (solo forward)
	apply := true
	if last, ok := n.PeerMgr.GetLastSeen(d.Peer); ok && time.Since(last) < n.FailureD.suspectTimeout {
		apply = false
	}

	// BLIND-COUNTER + rispetto di F
	n.rumorMu.Lock()
	cnt := n.deadSeenCnt[d.RumorID] + 1
	n.deadSeenCnt[d.RumorID] = cnt
	alreadyLeft := n.seenLeave[d.Peer]
	alreadyDead := n.handledDead[d.Peer]
	n.rumorMu.Unlock()

	if d.MaxFw > 0 && cnt > d.MaxFw {
		return
	}

	// Apply-once
	if apply && !alreadyLeft && !alreadyDead && cnt == 1 {
		log.Printf("DEAD %s — rumor da %s (%s)", d.Peer, env.From, d.RumorID)

		// effetti locali + tombstone meta
		if e, v, ok := n.Registry.RemoteMeta(d.Peer); ok {
			n.Registry.SaveTombMeta(d.Peer, e, v)
		}
		n.PeerMgr.Remove(d.Peer)
		n.FailureD.SuppressPeer(d.Peer)
		n.Registry.RemoveProvider(d.Peer)
		n.Registry.ResetRemoteMeta(d.Peer)
		n.updateQuorum()

		n.rumorMu.Lock()
		n.handledDead[d.Peer] = true
		n.rumorMu.Unlock()
	}

	// Forward GOSSIP per-hop (TTL--)
	if d.TTL > 0 {
		d.TTL--
		outD, _ := proto.Encode(proto.MsgDead, n.ID, d)
		targets := n.pickTargets(exclude(n.alivePeers(), env.From, d.Peer), int(d.Fanout))
		for _, p := range targets {
			n.GossipM.SendUDP(outD, p)
		}
		log.Printf("fwd DEAD %s TTL=%d B=%d seen=%d/%d", d.Peer, d.TTL, len(targets), cnt, d.MaxFw)
	}

}

// =============================
//  Handler: Leave rumor
// =============================

func (n *Node) handleLeave(env proto.Envelope, lv proto.Leave) {
	peer := lv.Peer
	rid := lv.RumorID
	if rid == "" {
		rid = fmt.Sprintf("leave|%s|%d|", peer, time.Now().UnixNano())
	}

	n.rumorMu.Lock()
	cnt := n.leaveSeenCnt[rid] + 1
	n.leaveSeenCnt[rid] = cnt
	already := n.seenLeave[peer]
	if !already {
		n.seenLeave[peer] = true
		// salva meta per guardia di revival
		if e, v, ok := n.Registry.RemoteMeta(peer); ok {
			n.Registry.SaveTombMeta(peer, e, v)
		}
	}
	n.rumorMu.Unlock()

	if lv.MaxFw > 0 && cnt > lv.MaxFw {
		return
	}

	// Apply-once (prima volta che lo vedo)
	if !already && cnt == 1 {
		n.PeerMgr.Remove(peer)
		n.FailureD.SuppressPeer(peer)
		n.Registry.RemoveProvider(peer)
		n.Registry.ResetRemoteMeta(peer)
		n.updateQuorum()
		log.Printf("LEAVE %s — applicato", peer)
	}

	// Forward GOSSIP per-hop
	if lv.TTL > 0 {
		lv.TTL--
		outL, _ := proto.Encode(proto.MsgLeave, n.ID, lv)
		targets := n.pickTargets(exclude(n.alivePeers(), env.From, peer), int(lv.Fanout))
		for _, p := range targets {
			n.GossipM.SendUDP(outL, p)
		}
		log.Printf("fwd LEAVE %s TTL=%d B=%d seen=%d/%d", peer, lv.TTL, len(targets), cnt, lv.MaxFw)
	}

}
