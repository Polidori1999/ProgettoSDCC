package node

import (
	"log"
	"strconv"
	"sync"
	"time"

	"ProgettoSDCC/pkg/proto"
)

// LookupManager: lookup SOLO gossip, con dedup e negative-cache.
type LookupManager struct {
	mu     sync.Mutex
	peers  *PeerManager
	reg    *ServiceRegistry
	gossip *GossipManager

	// stato minimo
	seenCnt  map[string]uint8     // ID → quante volte visto (dedup)
	pending  map[string]string    // reqID → service
	negCache map[string]time.Time // servizio inesistente fino a exp

	// policy
	learnFromResponses bool
	negCacheTTL        time.Duration
}

func NewLookupManager(pm *PeerManager, sr *ServiceRegistry, gm *GossipManager) *LookupManager {
	return &LookupManager{
		peers:    pm,
		reg:      sr,
		gossip:   gm,
		seenCnt:  make(map[string]uint8),
		pending:  make(map[string]string),
		negCache: make(map[string]time.Time),

		learnFromResponses: true,
		negCacheTTL:        30 * time.Second,
	}
}

// Abilita/disabilita il learning dalla LookupResponse (origin).
func (lm *LookupManager) SetLearnFromResponses(v bool) { lm.learnFromResponses = v }

// Invia lookup in modalità gossip.
// - ttl: hop residui (>=1)
// - fanout: peer random per hop (>=1)
// - dedupLimit: quante volte tollerare lo stesso ID (>=1)
func (lm *LookupManager) LookupGossip(service string, ttl, fanout, dedupLimit int) string {
	if ttl < 1 {
		ttl = 1
	}
	if fanout < 1 {
		fanout = 1
	}
	if dedupLimit < 1 {
		dedupLimit = 1
	}

	id := strconv.FormatInt(time.Now().UnixNano(), 10)

	peers := lm.peers.List()
	if len(peers) == 0 {
		log.Printf("[LOOKUP] no peers for %s", service)
		return id // nessun invio → nessuna pending (vedi sotto)
	}

	// clamp fanout
	if fanout > len(peers) {
		fanout = len(peers)
	}

	// prepara messaggio
	req := proto.LookupRequest{
		ID: id, Service: service, TTL: ttl, Origin: lm.gossip.self,
		Fanout: uint8(fanout),
		MaxFw:  uint8(dedupLimit), // riuso del campo come "dedupLimit"
	}
	msg, _ := proto.Encode(proto.MsgLookup, lm.gossip.self, req)

	// registra pending SOLO se stiamo per spedire davvero
	lm.mu.Lock()
	lm.pending[id] = service
	lm.mu.Unlock()

	// primo hop
	for _, p := range randomSubset(peers, fanout, lm.gossip.rnd) {
		lm.gossip.SendUDP(msg, p)
	}
	log.Printf("TX Lookup GOSSIP %s id=%s TTL=%d fanout=%d dedupLimit=%d",
		service, id, ttl, fanout, dedupLimit)
	return id
}

// Gestione richiesta in arrivo (dedup, reply se provider, forward, neg-cache).
func (lm *LookupManager) HandleRequest(env proto.Envelope) {
	lr, err := proto.DecodePayload[proto.LookupRequest](env.Data)
	if err != nil {
		log.Printf("bad LookupRequest: %v", err)
		return
	}
	log.Printf("RX Lookup %s id=%s TTL=%d from %s (fanout=%d dedupLimit=%d)",
		lr.Service, lr.ID, lr.TTL, env.From, lr.Fanout, lr.MaxFw)

	now := time.Now()
	peers := lm.peers.List()

	// negative-cache: se il servizio è noto-inesistente, interrompi
	lm.mu.Lock()
	if exp, ok := lm.negCache[lr.Service]; ok && now.Before(exp) {
		lm.mu.Unlock()
		return
	}

	// normalizza parametri minimi
	dedupLimit := int(lr.MaxFw)
	if dedupLimit < 1 {
		dedupLimit = 1
	}
	fanout := int(lr.Fanout)
	if fanout < 1 {
		fanout = 1
	}
	if fanout > len(peers) {
		fanout = len(peers)
	}

	// dedup per ID
	seen := lm.seenCnt[lr.ID] + 1
	lm.seenCnt[lr.ID] = seen
	lm.mu.Unlock()
	if int(seen) > dedupLimit {
		return
	}

	// se sono provider, rispondo (solo alla prima vista dell'ID)
	if provider, ok := lm.reg.Lookup(lr.Service); ok {
		if seen == 1 {
			resp := proto.LookupResponse{ID: lr.ID, Provider: provider}
			out, _ := proto.Encode(proto.MsgLookupResponse, lm.gossip.self, resp)
			lm.gossip.SendUDP(out, lr.Origin)
			log.Printf("  -> reply %s → %s (origin %s)", lr.Service, provider, lr.Origin)
		} else {
			log.Printf("  -> suppress duplicate reply (seen=%d) for %s", seen, lr.Service)
		}
		return
	}

	// forward o negative-cache a TTL esaurito
	if lr.TTL <= 0 {
		lm.mu.Lock()
		lm.negCache[lr.Service] = time.Now().Add(lm.negCacheTTL)
		lm.mu.Unlock()
		return
	}
	lr.TTL--

	out, _ := proto.Encode(proto.MsgLookup, lm.gossip.self, lr)

	// inoltra a 'fanout' peer, escludendo mittente e origin
	filtered := make([]string, 0, len(peers))
	for _, p := range peers {
		if p != env.From && p != lr.Origin {
			filtered = append(filtered, p)
		}
	}
	if len(filtered) == 0 {
		return
	}
	if fanout > len(filtered) {
		fanout = len(filtered)
	}

	for _, p := range randomSubset(filtered, fanout, lm.gossip.rnd) {
		lm.gossip.SendUDP(out, p)
	}
	log.Printf("  -> fwd id=%s TTL=%d fanout=%d seen=%d/%d",
		lr.ID, lr.TTL, fanout, seen, dedupLimit)
}

// Gestione risposta: aggiorna registry e chiude la pending.
func (lm *LookupManager) HandleResponse(env proto.Envelope) {
	lrsp, err := proto.DecodePayload[proto.LookupResponse](env.Data)
	if err != nil {
		return
	}
	log.Printf("RX LookupResponse id=%s provider=%s", lrsp.ID, lrsp.Provider)

	lm.mu.Lock()
	service, wasPending := lm.pending[lrsp.ID]
	if wasPending {
		delete(lm.pending, lrsp.ID)
	}
	lm.mu.Unlock()

	if !wasPending {
		return
	}

	if lm.learnFromResponses {
		lm.reg.Update(lrsp.Provider, []string{service})
		log.Printf("  -> registry updated: %s → %s", service, lrsp.Provider)
	} else {
		log.Printf("  -> learning-from-lookup DISABILITATO: salto update per %s", service)
	}
}
