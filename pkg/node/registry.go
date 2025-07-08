package node

import (
	"math/rand"
	"strings"
	"time"
)

// ServiceRegistry tiene traccia di quali provider offrono quali servizi.
type ServiceRegistry struct {
	Table map[string]map[string]time.Time // service → provider → lastSeen
}

// NewServiceRegistry crea un registry vuoto.
func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{
		Table: make(map[string]map[string]time.Time),
	}
}

// AddLocal aggiunge i servizi offerti da questo nodo.
func (sr *ServiceRegistry) AddLocal(selfID string, svcCSV string) {
	now := time.Now()
	for _, s := range strings.Split(svcCSV, ",") {
		if s = strings.TrimSpace(s); s != "" {
			if sr.Table[s] == nil {
				sr.Table[s] = make(map[string]time.Time)
			}
			sr.Table[s][selfID] = now
		}
	}
}

// Update aggiorna il timestamp dei servizi ricevuti in un heartbeat.
func (sr *ServiceRegistry) Update(provider string, services []string) {
	now := time.Now()
	for _, s := range services {
		if sr.Table[s] == nil {
			sr.Table[s] = make(map[string]time.Time)
		}
		sr.Table[s][provider] = now
	}
}

// RemoveProvider rimuove tutte le voci di provider (quando muore).
func (sr *ServiceRegistry) RemoveProvider(provider string) {
	for svc, provs := range sr.Table {
		delete(provs, provider)
		if len(provs) == 0 {
			delete(sr.Table, svc)
		}
	}
}

// Lookup restituisce un provider casuale per il servizio richiesto.
func (sr *ServiceRegistry) Lookup(service string) (string, bool) {
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
