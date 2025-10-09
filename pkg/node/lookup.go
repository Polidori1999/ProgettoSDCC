package node

import (
	"log"
	"strconv"
	"sync"
	"time"

	"ProgettoSDCC/pkg/proto"
)

// gestisco lookup via gossip con dedup e negative-cache.
// Tengo dipendenze minime: peers (routing), registry (per rispondere/aggiornare), gossip (per inviare).
type LookupManager struct {
	mu     sync.Mutex
	peers  *PeerManager
	reg    *ServiceRegistry
	gossip *GossipManager

	// Stato: contatore “quante volte ho visto quell’ID”, richieste pendenti originate da me,
	// e negative cache (servizio inesistente fino a una certa scadenza).
	seenCnt  map[string]uint8     // ount (uso uint8: basta e avanza per MaxFw piccolo)
	pending  map[string]string    // service (solo per le lookup avviate da me)
	negCache map[string]time.Time // service -> expire

	// Policy runtime
	learnFromResponses bool          // se true, imparo il mapping dal provider che risponde
	negCacheTTL        time.Duration // durata della negative cache
}

// Costruttore: inizializzo strutture e setto default prudente per negCacheTTL.
func NewLookupManager(pm *PeerManager, sr *ServiceRegistry, gm *GossipManager) *LookupManager {
	return &LookupManager{
		peers:    pm,
		reg:      sr,
		gossip:   gm,
		seenCnt:  make(map[string]uint8),
		pending:  make(map[string]string),
		negCache: make(map[string]time.Time),

		learnFromResponses: true,
		negCacheTTL:        30 * time.Second, //default
	}
}

func (lm *LookupManager) SetNegativeCacheTTL(d time.Duration) {
	if d > 0 {
		lm.negCacheTTL = d
	}
}

// Abilito/disabilito l’apprendimento dal provider quando sono io l’origin.
func (lm *LookupManager) SetLearnFromResponses(v bool) { lm.learnFromResponses = v }

// Avvio una lookup via gossip.
// ttl≥1 (hop budget), fanout≥1, dedupLimit≥1 (lo riuso come MaxFw per limitare i duplicati).
func (lm *LookupManager) LookupGossip(service string, ttl, fanout, dedupLimit int) string {
	// Normalizzo i parametri minimi
	if ttl < 1 {
		ttl = 1
	}
	if fanout < 1 {
		fanout = 1
	}
	if dedupLimit < 1 {
		dedupLimit = 1
	}

	// Genero un ID (timestamp ns) per dedup e match response
	id := strconv.FormatInt(time.Now().UnixNano(), 10)

	// Se non ho peer, loggo e ritorno comunque l’ID
	peers := lm.peers.List()
	if len(peers) == 0 {
		log.Printf("[LOOKUP] no peers for %s", service)
		return id
	}

	// Clamp fanout al numero di peer
	if fanout > len(peers) {
		fanout = len(peers)
	}

	// Preparo il messaggio. Riutilizzo MaxFw come “dedupLimit”
	req := proto.LookupRequest{
		ID: id, Service: service, TTL: ttl, Origin: lm.gossip.self,
		Fanout: uint8(fanout),
		MaxFw:  uint8(dedupLimit),
	}
	msg, _ := proto.Encode(proto.MsgLookup, lm.gossip.self, req)

	// Registro la pending: mi servirà per accettare solo la prima risposta al mio ID
	lm.mu.Lock()
	lm.pending[id] = service
	lm.mu.Unlock()

	// Primo hop: scelgo fanout peer random e invio
	for _, p := range randomSubset(peers, fanout, lm.gossip.rnd) {
		lm.gossip.SendUDP(msg, p)
	}
	log.Printf("TX Lookup GOSSIP %s id=%s TTL=%d fanout=%d dedupLimit=%d",
		service, id, ttl, fanout, dedupLimit)
	return id
}

// Gestisco una richiesta entrante: dedup, rispondo se sono provider, altrimenti forward,
// e se TTL finisce metto in negative-cache per un po’.
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

	// Negative-cache: se so già che questo servizio non esistde.
	lm.mu.Lock()
	if exp, ok := lm.negCache[lr.Service]; ok && now.Before(exp) {
		log.Printf("NEG-CACHE drop svc=%s id=%s from=%s until=%s node=%s",
			lr.Service, lr.ID, env.From, exp.Format(time.RFC3339), lm.gossip.self)
		lm.mu.Unlock()
		return
	}
	// Se esiste ma è scaduta, la tolgo e continuo
	if exp, ok := lm.negCache[lr.Service]; ok && now.After(exp) {
		delete(lm.negCache, lr.Service)
		log.Printf("NEG-CACHE expire svc=%s node=%s", lr.Service, lm.gossip.self)
	}

	// Normalizzo parametri minimi e clamp
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

	// Dedup per ID: aumento il contatore per questa richiesta
	seen := lm.seenCnt[lr.ID] + 1
	lm.seenCnt[lr.ID] = seen
	lm.mu.Unlock()
	if int(seen) > dedupLimit {
		return
	}

	// Se sono provider, rispondo (solo la prima volta che vedo quell’ID)
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

	// Non sono provider. se ttl finito allora metto in negative-cache e basta.
	if lr.TTL <= 0 {
		lm.mu.Lock()
		lm.negCache[lr.Service] = time.Now().Add(lm.negCacheTTL)
		lm.mu.Unlock()
		return
	}
	lr.TTL--

	out, _ := proto.Encode(proto.MsgLookup, lm.gossip.self, lr)

	// Inoltro a 'fanout' peer, escludendo mittente e origin per evitare ping-pong
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

// Gestisco una risposta: aggiorno la registry (se voglio imparare) e chiudo la pending.
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
		return // non l’ho originata io → ignoro
	}

	if lm.learnFromResponses {
		lm.reg.Update(lrsp.Provider, []string{service})
		log.Printf("  -> registry updated: %s → %s", service, lrsp.Provider)
	} else {
		log.Printf("  -> learning-from-lookup DISABILITATO: salto update per %s", service)
	}
}
