package node

import (
	"fmt"
	"log"
	"math"
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
}

// NewLookupManager costruisce e restituisce un LookupManager
func NewLookupManager(pm *PeerManager, sr *ServiceRegistry, gm *GossipManager) *LookupManager {
	return &LookupManager{
		seenCnt:            make(map[string]uint8),
		negCache:           make(map[string]time.Time),
		pending:            make(map[string]string),
		peers:              pm,
		reg:                sr,
		gossip:             gm,
		learnFromResponses: true, // default: comportamento attuale
	}
}

// SetLearnFromResponses abilita/disabilita il learning dalla LookupResponse.
// Se false, il nodo originante NON memorizza servizio→provider.
func (lm *LookupManager) SetLearnFromResponses(v bool) { lm.learnFromResponses = v }

// avvia una ricerca per `service` con TTL iniziale `ttl`.
func (lm *LookupManager) Lookup(service string, ttl int) {
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	lm.mu.Lock()
	lm.pending[id] = service
	lm.mu.Unlock()

	req := proto.LookupRequest{
		ID: id, Service: service, TTL: ttl, Origin: lm.gossip.self,
		Fanout: 0, MaxFw: 0, // ← flooding mode
	}
	msg, _ := proto.Encode(proto.MsgLookup, lm.gossip.self, req)

	peers := lm.peers.List()
	if len(peers) == 0 {
		log.Printf("no peers for lookup %s", service)
		return
	}

	k := int(math.Ceil(math.Log2(float64(len(peers)))))
	if k < 1 {
		k = 1
	}
	for _, p := range randomSubset(peers, k, lm.gossip.rnd) {
		lm.gossip.SendUDP(msg, p)
	}
	log.Printf("TX Lookup FLOOD %s TTL=%d → k=%d peers", service, ttl, k)
}
func (lm *LookupManager) LookupGossip(service string, ttl, fanout, maxfw int) string {
	if ttl < 1 {
		ttl = 1
	}
	if fanout < 1 {
		fanout = 1
	}
	if maxfw < 1 {
		maxfw = 1
	}

	id := fmt.Sprintf("%d", time.Now().UnixNano())
	lm.mu.Lock()
	lm.pending[id] = service
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
	lr, err := proto.DecodePayload[proto.LookupRequest](env.Data)
	if err != nil {
		log.Printf("bad LookupRequest: %v", err)
		return
	}
	log.Printf("RX Lookup %s TTL=%d from %s (B=%d F=%d)", lr.Service, lr.TTL, env.From, lr.Fanout, lr.MaxFw)

	// 0) negative cache
	lm.mu.Lock()
	if t, ok := lm.negCache[lr.Service]; ok && time.Now().Before(t) {
		lm.mu.Unlock()
		return
	}
	// 1) dedup/contatore
	seen := lm.seenCnt[lr.ID]
	if lr.MaxFw == 0 {
		// FLOODING: singolo passaggio (bool-like)
		if seen >= 1 {
			lm.mu.Unlock()
			return
		}
		lm.seenCnt[lr.ID] = 1
	} else {
		// GOSSIP: incremento contatore e stoppo se supero F
		seen++
		lm.seenCnt[lr.ID] = seen
		if seen > lr.MaxFw {
			lm.mu.Unlock()
			return
		}
	}
	lm.mu.Unlock()

	// 2) se sono provider, rispondo all'origin con LookupResponse
	if provider, ok := lm.reg.Lookup(lr.Service); ok {
		if lr.MaxFw == 0 || seen == 1 { // flooding: sempre; gossip: solo prima vista
			resp := proto.LookupResponse{ID: lr.ID, Provider: provider}
			out, _ := proto.Encode(proto.MsgLookupResponse, lm.gossip.self, resp)
			lm.gossip.SendUDP(out, lr.Origin)
			log.Printf("  -> replied %s → %s (origin %s)", lr.Service, provider, lr.Origin)
		} else {
			log.Printf("  -> suppress duplicate reply (seen=%d) for %s", seen, lr.Service)
		}
		return // niente forward dal provider (riduce traffico)
	}

	// 3) forward se TTL>0
	if lr.TTL <= 0 {
		// TTL esaurito → negative cache
		lm.mu.Lock()
		lm.negCache[lr.Service] = time.Now().Add(30 * time.Second)
		lm.mu.Unlock()
		return
	}
	lr.TTL--
	out, _ := proto.Encode(proto.MsgLookup, lm.gossip.self, lr)

	peers := lm.peers.List()
	if len(peers) == 0 {
		return
	}

	if lr.MaxFw == 0 {
		// FLOODING: k = ceil(log2(N))
		k := int(math.Ceil(math.Log2(float64(len(peers)))))
		if k < 1 {
			k = 1
		}
		for _, p := range randomSubset(peers, k, lm.gossip.rnd) {
			if p == env.From || p == lr.Origin {
				continue
			}
			lm.gossip.SendUDP(out, p)
		}
		log.Printf("  -> fwd FLOOD TTL=%d k=%d", lr.TTL, k)
	} else {
		// GOSSIP: fwd a B peer random
		B := int(lr.Fanout)
		if B < 1 {
			B = 1
		}
		if B > len(peers) {
			B = len(peers)
		}
		// escludo mittente e origin
		filtered := make([]string, 0, len(peers))
		for _, p := range peers {
			if p != env.From && p != lr.Origin {
				filtered = append(filtered, p)
			}
		}
		for _, p := range randomSubset(filtered, B, lm.gossip.rnd) {
			lm.gossip.SendUDP(out, p)
		}
		log.Printf("  -> fwd GOSSIP TTL=%d B=%d seen=%d/%d", lr.TTL, B, seen, lr.MaxFw)
	}
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
