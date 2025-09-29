package node

import (
	"fmt"
	"log"
	"sync"
	"time"

	"ProgettoSDCC/pkg/proto"
)

type LookupManager struct {
	mu                 sync.Mutex
	seenCnt            map[string]uint8     // dedup delle richieste in arrivo
	negCache           map[string]time.Time // negative-cache TTL per servizio
	pending            map[string]string    // id → nome del servizio in corso di lookup
	peers              *PeerManager
	reg                *ServiceRegistry
	gossip             *GossipManager
	learnFromResponses bool
	negCacheTTL        time.Duration

	pendingAt map[string]time.Time // quando è partita la richiesta (per ripulire)
	seenAt    map[string]time.Time // quando abbiamo visto l’ID l’ultima volta (per ripulire)

	pendingTTL time.Duration // es. quanto tenere una pending “appesa”
	seenTTL    time.Duration // es. quanto tenere gli ID visti per dedup
}

// NewLookupManager costruisce e restituisce un LookupManager
func NewLookupManager(pm *PeerManager, sr *ServiceRegistry, gm *GossipManager) *LookupManager {
	return &LookupManager{
		seenCnt:            make(map[string]uint8),
		seenAt:             make(map[string]time.Time),
		negCache:           make(map[string]time.Time),
		pending:            make(map[string]string),
		pendingAt:          make(map[string]time.Time),
		peers:              pm,
		reg:                sr,
		gossip:             gm,
		learnFromResponses: true, // default: comportamento attuale

		negCacheTTL: 30 * time.Second, // sostituisce l'hardcoded
		pendingTTL:  10 * time.Second, // nuovo default
		seenTTL:     2 * time.Minute,  // nuovo default
	}

}

// SetLearnFromResponses abilita/disabilita il learning dalla LookupResponse.
// Se false, il nodo originante NON memorizza servizio→provider.
func (lm *LookupManager) SetLearnFromResponses(v bool) { lm.learnFromResponses = v }

func (lm *LookupManager) LookupGossip(service string, ttl, fanout, maxfw int) string {
	lm.GC()

	if ttl < 1 {
		ttl = 1
	}
	if fanout < 1 {
		fanout = 1
	}
	if maxfw < 1 {
		maxfw = 1
	}

	now := time.Now()
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	lm.mu.Lock()
	lm.pending[id] = service
	lm.pendingAt[id] = now
	lm.mu.Unlock()

	req := proto.LookupRequest{
		ID: id, Service: service, TTL: ttl, Origin: lm.gossip.self,
		Fanout: uint8(fanout), MaxFw: uint8(maxfw), // ← gossip mode
	}
	msg, _ := proto.Encode(proto.MsgLookup, lm.gossip.self, req)

	peers := lm.peers.List()
	if len(peers) == 0 {
		log.Printf("no peers for lookup %s", service)
		return id
	}

	// in gossip il primo hop va già a B peer random (NON k log N)
	B := fanout
	if B > len(peers) {
		B = len(peers)
	}
	for _, p := range randomSubset(peers, B, lm.gossip.rnd) {
		lm.gossip.SendUDP(msg, p)
	}
	log.Printf("TX Lookup GOSSIP %s TTL=%d B=%d F=%d", service, ttl, fanout, maxfw)
	return id
}

// processa un MsgLookup in arrivo (forwarding & negative-cache)

func (lm *LookupManager) HandleRequest(env proto.Envelope) {
	lm.GC()

	lr, err := proto.DecodePayload[proto.LookupRequest](env.Data)
	if err != nil {
		log.Printf("bad LookupRequest: %v", err)
		return
	}
	log.Printf("RX Lookup %s TTL=%d from %s (B=%d F=%d)", lr.Service, lr.TTL, env.From, lr.Fanout, lr.MaxFw)

	now := time.Now()

	// 0) negative cache
	lm.mu.Lock()
	if exp, ok := lm.negCache[lr.Service]; ok && now.Before(exp) {
		lm.mu.Unlock()
		return
	}

	// === Normalizza parametri per compatibilità con "vecchie" richieste ===
	// - prima: MaxFw==0 → branch "flooding" booleano; equivale a F=1
	F := int(lr.MaxFw)
	if F <= 0 {
		F = 1
	}
	// - prima: Fanout==0 → k = ceil(log2(N))
	peers := lm.peers.List()
	B := int(lr.Fanout)
	if B <= 0 {
		B = logFanout(len(peers))
	}
	if B < 1 {
		B = 1
	}
	if B > len(peers) {
		B = len(peers)
	}

	// 1) dedup/contatore
	seen := lm.seenCnt[lr.ID] + 1
	lm.seenCnt[lr.ID] = seen
	lm.seenAt[lr.ID] = now
	if int(seen) > F {
		lm.mu.Unlock()
		return
	}
	lm.mu.Unlock()

	// 2) se sono provider, rispondo (solo la prima volta che vedo l'ID)
	if provider, ok := lm.reg.Lookup(lr.Service); ok {
		if seen == 1 {
			resp := proto.LookupResponse{ID: lr.ID, Provider: provider}
			out, _ := proto.Encode(proto.MsgLookupResponse, lm.gossip.self, resp)
			lm.gossip.SendUDP(out, lr.Origin)
			log.Printf("  -> replied %s → %s (origin %s)", lr.Service, provider, lr.Origin)
		} else {
			log.Printf("  -> suppress duplicate reply (seen=%d) for %s", seen, lr.Service)
		}
		return
	}

	// 3) forward se TTL>0, altrimenti negativa
	if lr.TTL <= 0 {
		lm.mu.Lock()
		lm.negCache[lr.Service] = time.Now().Add(lm.negCacheTTL)
		lm.mu.Unlock()
		return
	}
	lr.TTL--
	out, _ := proto.Encode(proto.MsgLookup, lm.gossip.self, lr)

	// escludo mittente e origin
	filtered := make([]string, 0, len(peers))
	for _, p := range peers {
		if p != env.From && p != lr.Origin {
			filtered = append(filtered, p)
		}
	}
	if len(filtered) == 0 {
		return
	}
	if B > len(filtered) {
		B = len(filtered)
	}
	for _, p := range randomSubset(filtered, B, lm.gossip.rnd) {
		lm.gossip.SendUDP(out, p)
	}
	log.Printf("  -> fwd TTL=%d B=%d seen=%d/%d", lr.TTL, B, seen, F)
}

// processa un MsgLonse, aggiorna registry e cancella la pending
func (lm *LookupManager) HandleResponse(env proto.Envelope) {
	lrsp, err := proto.DecodePayload[proto.LookupResponse](env.Data)
	if err != nil {
		return
	}
	log.Printf("RX LookupResponse %s → %s", lrsp.ID, lrsp.Provider)

	lm.mu.Lock()
	service, wasPending := lm.pending[lrsp.ID]
	if wasPending {
		delete(lm.pending, lrsp.ID)
		delete(lm.pendingAt, lrsp.ID)
	}
	lm.mu.Unlock()

	if !wasPending {
		return // evita update “vuoti”
	}

	if lm.learnFromResponses {
		// aggiorna la mappa servizio→provider SOLO se abilitato
		lm.reg.Update(lrsp.Provider, []string{service})
		log.Printf("  -> registry updated: %s → %s", service, lrsp.Provider)
	} else {
		// learning disattivato: niente caching dal lato origin
		log.Printf("  -> learning-from-lookup DISABILITATO: salto update per %s", service)
	}
}
func (lm *LookupManager) pruneNegCache(now time.Time) {
	for svc, exp := range lm.negCache {
		if now.After(exp) {
			delete(lm.negCache, svc)
		}
	}
}
func (lm *LookupManager) prunePending(now time.Time) {
	for id, t0 := range lm.pendingAt {
		if now.Sub(t0) > lm.pendingTTL {
			delete(lm.pendingAt, id)
			delete(lm.pending, id)
		}
	}
}
func (lm *LookupManager) pruneSeen(now time.Time) {
	cutoff := now.Add(-lm.seenTTL)
	for id, t0 := range lm.seenAt {
		if t0.Before(cutoff) {
			delete(lm.seenAt, id)
			delete(lm.seenCnt, id)
		}
	}
}
func (lm *LookupManager) GC() {
	lm.mu.Lock()
	now := time.Now()
	lm.pruneNegCache(now)
	lm.prunePending(now)
	lm.pruneSeen(now)
	lm.mu.Unlock()
}
