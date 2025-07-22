package node

import (
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

func (sr *ServiceRegistry) AddLocal(selfID, svcCSV string) {
	now := time.Now()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	// resettiamo la slice ogni volta (nel caso venisse richiamata più volte)
	sr.localServices = nil

	for _, s := range strings.Split(svcCSV, ",") {
		if s = strings.TrimSpace(s); s == "" {
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

// Lookup restituisce un provider casuale per il servizio.
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

// ───────────────────────────────
// *** NOVITÀ ***  snapshot & delta
// ───────────────────────────────

// Snapshot restituisce una *copia* leggera: mappa[servizio][]provider.
func (sr *ServiceRegistry) Snapshot() map[string][]string {
	out := make(map[string][]string)

	sr.mu.RLock()
	defer sr.mu.RUnlock()

	for svc, providers := range sr.Table {
		for p := range providers {
			out[svc] = append(out[svc], p)
		}
	}
	return out
}

// ApplyDelta integra nel registry le entry che mancano.
// Per semplicità: timbriamo sempre lastSeen = now (non facciamo merge timestamp).
func (sr *ServiceRegistry) ApplyDelta(delta map[string][]string) {
	now := time.Now()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	for svc, provs := range delta {
		if sr.Table[svc] == nil {
			sr.Table[svc] = make(map[string]time.Time)
		}
		for _, p := range provs {
			if _, ok := sr.Table[svc][p]; !ok {
				sr.Table[svc][p] = now
			}
		}
	}
}
