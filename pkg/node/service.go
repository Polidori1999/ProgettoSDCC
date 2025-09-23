package node

import (
	"ProgettoSDCC/pkg/proto"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// ───────────────────────────────────────────────────────────────────────────────
// ServiceRegistry tiene traccia di quali provider offrono quali servizi.
// - Tabella: service → provider → lastSeen
// - mu: protezione concorrenza (HB, lookup, anti-entropy possono arrivare insieme)
// ───────────────────────────────────────────────────────────────────────────────

type ServiceRegistry struct {
	mu            sync.RWMutex
	Table         map[string]map[string]time.Time
	localServices []string //  ← nuovo campo
}

func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{
		Table: make(map[string]map[string]time.Time),
	}
}

// normalizza il nome del servizio
func normSvc(s string) string {
	return strings.TrimSpace(strings.ToLower(s))
}

// ritorna l'indice del servizio nella slice, -1 se non presente
func findSvc(ss []string, svc string) int {
	for i, x := range ss {
		if x == svc {
			return i
		}
	}
	return -1
}
func (sr *ServiceRegistry) AddLocal(selfID, svcCSV string) {
	now := time.Now()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	// resettiamo la slice ogni volta (nel caso venisse richiamata più volte)
	sr.localServices = nil

	for _, raw := range strings.Split(svcCSV, ",") {
		s := strings.TrimSpace(strings.ToLower(raw))
		if s == "" {
			continue
		}
		if !proto.IsValidService(s) {
			// Non inserire servizi sconosciuti
			// log.Printf("[WARN] servizio non valido: %q (ignorato)", s)
			continue
		}
		if sr.Table[s] == nil {
			sr.Table[s] = make(map[string]time.Time)
		}
		sr.Table[s][selfID] = now
		sr.localServices = append(sr.localServices, s)
	}
}

// LocalServices restituisce **una copia** della slice (per sicurezza).
func (sr *ServiceRegistry) LocalServices() []string {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	dup := make([]string, len(sr.localServices))
	copy(dup, sr.localServices)
	return dup
}

// Update: usato quando si riceve info da fuori (HB o anti-entropy).
func (sr *ServiceRegistry) Update(provider string, services []string) {
	now := time.Now()
	sr.mu.Lock()
	defer sr.mu.Unlock()

	for _, s := range services {
		if sr.Table[s] == nil {
			sr.Table[s] = make(map[string]time.Time)
		}
		sr.Table[s][provider] = now
	}
}

// RemoveProvider: cancella totalmente un provider morto/left.
func (sr *ServiceRegistry) RemoveProvider(provider string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	for svc, provs := range sr.Table {
		delete(provs, provider)
		if len(provs) == 0 {
			delete(sr.Table, svc)
		}
	}
}

// restituisce un provider casuale per il servizio.
func (sr *ServiceRegistry) Lookup(service string) (string, bool) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	provs, ok := sr.Table[service]
	if !ok || len(provs) == 0 {
		return "", false
	}
	// scelta casuale uniforme
	i := rand.Intn(len(provs))
	j := 0
	for p := range provs {
		if j == i {
			return p, true
		}
		j++
	}
	return "", false
}

// Aaggiunge un servizio locale (selfID = id del nodo).
// Ritorna true se ha cambiato lo stato (nuovo servizio), false se già presente.
func (sr *ServiceRegistry) AddLocalService(selfID, svc string) bool {
	svc = normSvc(svc)
	if svc == "" || !proto.IsValidService(svc) {
		return false
	}
	now := time.Now()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	if findSvc(sr.localServices, svc) >= 0 {
		// già presente: aggiorno solo lastSeen in Table per robustezza
		if sr.Table[svc] == nil {
			sr.Table[svc] = make(map[string]time.Time)
		}
		sr.Table[svc][selfID] = now
		return false
	}

	// aggiungi al locale
	sr.localServices = append(sr.localServices, svc)

	// e alla tabella globale service->providers
	if sr.Table[svc] == nil {
		sr.Table[svc] = make(map[string]time.Time)
	}
	sr.Table[svc][selfID] = now
	return true
}

// RemoveLocalService rimuove un servizio locale (selfID = id del nodo).
// Ritorna true se ha cambiato lo stato (era presente), false se era già assente.
func (sr *ServiceRegistry) RemoveLocalService(selfID, svc string) bool {
	svc = normSvc(svc)
	if svc == "" {
		return false
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	idx := findSvc(sr.localServices, svc)
	if idx < 0 {
		return false
	}

	// rimuovi dallo slice locale (swap con ultimo per O(1))
	last := len(sr.localServices) - 1
	sr.localServices[idx], sr.localServices[last] = sr.localServices[last], sr.localServices[idx]
	sr.localServices = sr.localServices[:last]

	// rimuovi SOLO il provider locale dalla tabella
	if provs, ok := sr.Table[svc]; ok {
		delete(provs, selfID)
		if len(provs) == 0 {
			delete(sr.Table, svc)
		}
	}
	return true
}
