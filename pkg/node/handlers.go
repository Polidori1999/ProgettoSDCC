package node

import (
	"ProgettoSDCC/pkg/proto"
	"fmt"
	"log"
	"strings"
	"time"
)

// uso la "guardia tombstone" per gestire rientri.
func (n *Node) applyRevivalIfNewer(peer string, epoch int64, ver uint64) bool {
	if eT, vT, hadTomb := n.Registry.TombMeta(peer); hadTomb {
		// Se non è più nuovo del tombstone, ignoro l'HB (revival non valido)
		if !(epoch > eT || (epoch == eT && ver > vT)) {
			return false
		}
		// Revival: pulisco stato locale e via le soppressioni
		n.Registry.ClearTombMeta(peer)

		n.rumorMu.Lock()
		delete(n.handledDead, peer)
		delete(n.seenLeave, peer)
		n.suspectCount[peer] = 0
		// pulisco tutte le tracce di rumor per quel peer
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
		for id := range n.suspectSeenCnt {
			if strings.HasPrefix(id, "suspect|"+peer+"|") {
				delete(n.suspectSeenCnt, id)
			}
		}
		n.rumorMu.Unlock()

		n.FailureD.UnsuppressPeer(peer)
		log.Printf("Peer %s RI-ENTRATO (revival) epoch=%d ver=%d", peer, epoch, ver)
	}
	return true
}

// se sto imparando da HB (learnFromHB=true), rispondo a una RepairReq
// con un HB full (epoch, ver, miei servizi, lista peer). Altrimenti loggo e ignoro.
func (n *Node) handleRepairReq(env proto.Envelope) {
	if !n.learnFromHB {
		log.Printf("[REPAIR] request from %s ignorata (learnHB=false)", env.From)
		return
	}
	if _, err := proto.DecodePayload[proto.RepairReq](env.Data); err != nil {
		// Payload malformato: la ignoro in silenzio (best-effort)
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

// applico revival guard; imparo peer via piggyback (saltando tombstoned);
// se il meta remoto è più nuovo richiedo un REPAIR (solo se learnFromHB=true).
func (n *Node) handleHeartbeatLight(env proto.Envelope, hbd proto.HeartbeatLight) {
	peer := env.From

	// Guardia tombstone/riammissione
	if !n.applyRevivalIfNewer(peer, hbd.Epoch, hbd.SvcVer) {
		return
	}

	// Piggyback: imparo peer "hint" saltando quelli tombstoned/left, sia lato Node che lato Registry
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

	// Se il meta remoto è più nuovo, chiedo un REPAIR (HB full) solo se sto imparando da HB
	if e0, v0, ok := n.Registry.RemoteMeta(peer); !ok || hbd.Epoch > e0 || (hbd.Epoch == e0 && hbd.SvcVer > v0) {
		if n.learnFromHB {
			req := proto.RepairReq{Nonce: time.Now().UnixNano()}
			out, _ := proto.Encode(proto.MsgRepairReq, n.ID, req)
			n.GossipM.SendUDP(out, peer)
		} else {
			log.Printf("[REPAIR] skip: learnHB=false → non richiedo HB(full) a %s (epoch=%d ver=%d)", peer, hbd.Epoch, hbd.SvcVer)
		}
	}

	// Aggiorno vista locale e quorum
	n.PeerMgr.Add(peer)
	n.PeerMgr.Seen(peer)
	n.updateQuorum()
	log.Printf("HB(light) from %-12s peers=%v", peer, hbd.Peers)
}

// come il light ma con servizi + lista peer completa.
// Normalizzo i servizi, applico solo se epoch/ver sono nuovi e learnFromHB=true.
func (n *Node) handleHeartbeatFull(env proto.Envelope, hb proto.Heartbeat) {
	peer := env.From

	// Guardia tombstone/riammissione
	if !n.applyRevivalIfNewer(peer, hb.Epoch, hb.SvcVer) {
		return
	}

	// Piggyback: imparo peer dalla lista completa, saltando tombstoned/left
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

	// Aggiorno vista locale e quorum
	n.PeerMgr.Add(peer)
	n.PeerMgr.Seen(peer)
	n.updateQuorum()

	// Normalizzo servizi e filtro quelli validi
	valid := make([]string, 0, len(hb.Services))
	for _, s := range hb.Services {
		s = strings.TrimSpace(strings.ToLower(s))
		if proto.IsValidService(s) {
			valid = append(valid, s)
		}
	}

	// Applico update servizi solo se:
	// - sto imparando da HB
	// - epoch/ver sono nuovi
	if n.learnFromHB {
		if n.Registry.UpdateWithVersion(peer, valid, hb.Epoch, hb.SvcVer) {
			log.Printf("HB(full)   from %-12s epoch=%d ver=%d services=%v peers=%v", peer, hb.Epoch, hb.SvcVer, valid, hb.Peers)
		} else {
			log.Printf("HB(full)   from %-12s ignorato (epoch/ver non nuovi)", peer)
		}
	} else {
		log.Printf("HB(full)   from %-12s learning DISABILITATO → salto update servizi (epoch=%d ver=%d)", peer, hb.Epoch, hb.SvcVer)
	}
}

// Delego la logica di request/response al lookup man(tiene TTL, dedup, ecc.)
func (n *Node) handleLookup(env proto.Envelope, lm *LookupManager) {
	lm.HandleRequest(env)
}
func (n *Node) handleLookupResponse(env proto.Envelope, lm *LookupManager) {
	lm.HandleResponse(env)
}

// al primo avvisatmen ncremento il "voto" per quel peer; se raggiungo il quorum locale dichiaro DEAD (una volta sola),
// emetto il DeadRumor e applico effetti locali (tombstone, remove, suppression, ecc.).
// In ogni caso, se TTL>0, inoltro il rumor (TTL--), escludendo mittente e peer target.
func (n *Node) handleSuspect(env proto.Envelope, r proto.SuspectRumor) {
	// Conteggio seen per rispetto F (MaxFw): se supero, mi fermo
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

	// 1) Se l'ho visto di recente, non considero il voto (e non forwardo il rumor).
	if last, ok := n.PeerMgr.GetLastSeen(r.Peer); ok && time.Since(last) < n.FailureD.suspectTimeout/2 {
		return
	}
	// 2) Probe rapida best-effort: invio un RepairReq al target e aspetto un breve ack indiretto
	//    (il target risponde con HB(full) grazie a handleRepairReq → aggiorna LastSeen).
	if n.quickProbe(r.Peer, 400*time.Millisecond) {
		return
	}
	// Prima vista  conto voto e controllo quorum
	n.rumorMu.Lock()
	first := !n.seenSuspect[r.RumorID]
	if first {
		n.seenSuspect[r.RumorID] = true
		n.suspectCount[r.Peer]++
	}
	votes := n.suspectCount[r.Peer]
	need := n.quorumThreshold
	n.rumorMu.Unlock()

	// Se raggiungo quorum e non ho ancora marcato DEAD, applico DEAD una sola volta
	if first && votes >= need && !n.handledDead[r.Peer] {
		// Ulteriore guardia di "freschezza": se per me è fresco, non promuovo a DEAD.
		if last, ok := n.PeerMgr.GetLastSeen(r.Peer); ok && time.Since(last) < n.FailureD.suspectTimeout {
			return
		}
		n.rumorMu.Lock()
		n.handledDead[r.Peer] = true
		n.rumorMu.Unlock()
		log.Printf("Peer %s DEAD (quorum %d raggiunto)", r.Peer, need)

		// DeadRumor verso B peer vivi (escludo il morto)
		tsNow := time.Now().UnixNano()
		d := proto.DeadRumor{
			RumorID: fmt.Sprintf("dead|%s|%d", r.Peer, tsNow),
			Peer:    r.Peer,
			Fanout:  uint8(n.fdB),
			MaxFw:   uint8(n.fdF),
			TTL:     uint8(n.fdT),
		}
		outD, _ := proto.Encode(proto.MsgDead, n.ID, d)
		for _, p := range n.pickTargets(exclude(n.alivePeers(), r.Peer), n.fdB) {
			n.GossipM.SendUDP(outD, p)
		}

		// Effetti locali + tombstone meta per guardia revival
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

	// Forward per-hop (TTL--)
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

// rispetto F, applico una sola volta gli effetti locali (se per me non è "fresco"),
// poi inoltro se TTL>0. Uso la "freschezza" per evitare di uccidere peer che ho visto da pochissimo.
func (n *Node) handleDead(env proto.Envelope, d proto.DeadRumor) {
	// Filtro freschezza: se l'ho visto di recente (prima di suspectTimeout), non applico (solo forward)
	apply := true
	if last, ok := n.PeerMgr.GetLastSeen(d.Peer); ok && time.Since(last) < n.FailureD.suspectTimeout {
		apply = false
	}

	// Conteggio cieco e rispetto MaxFw
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

		// Effetti locali + tombstone
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

	// Forward se ho ancora TTL
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

// applico una sola volta, salvo tombstone per revival guard,
// rimuovo peer e sopprimo FD; rispetto MaxFw e TTL durante il forward.
func (n *Node) handleLeave(env proto.Envelope, lv proto.Leave) {
	peer := lv.Peer
	rid := lv.RumorID
	if rid == "" {
		rid = fmt.Sprintf("leave|%s|%d|", peer, time.Now().UnixNano())
	}

	// Seen count per F, e prima volta segno seenLeave[peer]=true
	n.rumorMu.Lock()
	cnt := n.leaveSeenCnt[rid] + 1
	n.leaveSeenCnt[rid] = cnt
	already := n.seenLeave[peer]
	if !already {
		n.seenLeave[peer] = true
		// Salvo meta per guardia revival
		if e, v, ok := n.Registry.RemoteMeta(peer); ok {
			n.Registry.SaveTombMeta(peer, e, v)
		}
	}
	n.rumorMu.Unlock()

	if lv.MaxFw > 0 && cnt > lv.MaxFw {
		return
	}

	// Apply-once
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

// invia un RepairReq al target e attende per un breve intervallo
// che il LastSeen del target avanzi (segno che ci ha risposto con un HB(full)).
// Restituisce true se ha avuto segni di vita entro 'wait'.
func (n *Node) quickProbe(target string, wait time.Duration) bool {
	// Timestamp di riferimento
	t0, _ := n.PeerMgr.GetLastSeen(target)

	// RepairReq come "ping" best-effort
	req := proto.RepairReq{Nonce: time.Now().UnixNano()}
	out, _ := proto.Encode(proto.MsgRepairReq, n.ID, req)
	n.GossipM.SendUDP(out, target)

	deadline := time.Now().Add(wait)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		if time.Now().After(deadline) {
			return false
		}
		<-ticker.C
		if ts, ok := n.PeerMgr.GetLastSeen(target); ok {
			// Avanzato rispetto a prima ed entro la finestra "sano" → vivo
			if ts.After(t0) && time.Since(ts) < n.FailureD.suspectTimeout {
				return true
			}
		}
	}
}
