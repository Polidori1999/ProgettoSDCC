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
	localEpoch    int64
	localVersion  uint64
	remoteMeta    map[string]svcMeta // provider -> (epoch,ver)
	tombMeta      map[string]svcMeta // peer -> (epoch,version) al momento della tombstone

}
type svcMeta struct {
	Epoch   int64
	Version uint64
}

func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{
		Table:      make(map[string]map[string]time.Time),
		localEpoch: time.Now().UnixNano(),
		remoteMeta: make(map[string]svcMeta),
		tombMeta:   make(map[string]svcMeta), // NEW
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
		if sr.Table[svc] == nil {
			sr.Table[svc] = make(map[string]time.Time)
		}
		sr.Table[svc][selfID] = now
		return false
	}

	sr.localServices = append(sr.localServices, svc)
	if sr.Table[svc] == nil {
		sr.Table[svc] = make(map[string]time.Time)
	}
	sr.Table[svc][selfID] = now

	sr.localVersion++ // ← QUI: bump solo su vero cambiamento
	return true
}

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

	last := len(sr.localServices) - 1
	sr.localServices[idx], sr.localServices[last] = sr.localServices[last], sr.localServices[idx]
	sr.localServices = sr.localServices[:last]

	if provs, ok := sr.Table[svc]; ok {
		delete(provs, selfID)
		if len(provs) == 0 {
			delete(sr.Table, svc)
		}
	}

	sr.localVersion++ // ← QUI
	return true
}

func (sr *ServiceRegistry) LocalEpoch() int64 {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return sr.localEpoch
}
func (sr *ServiceRegistry) LocalVersion() uint64 {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return sr.localVersion
}

func (sr *ServiceRegistry) UpdateWithVersion(provider string, services []string, epoch int64, ver uint64) bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	cur := sr.remoteMeta[provider]
	newer := (epoch > cur.Epoch) || (epoch == cur.Epoch && ver > cur.Version)
	if !newer {
		return false
	}

	// riscrivi lo stato del provider
	now := time.Now()
	for svc, provs := range sr.Table {
		delete(provs, provider)
		if len(provs) == 0 {
			delete(sr.Table, svc)
		}
	}
	for _, s := range services {
		if sr.Table[s] == nil {
			sr.Table[s] = make(map[string]time.Time)
		}
		sr.Table[s][provider] = now
	}
	sr.remoteMeta[provider] = svcMeta{Epoch: epoch, Version: ver}
	return true
}

func (sr *ServiceRegistry) RemoteMeta(provider string) (epoch int64, ver uint64, ok bool) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	m, ok := sr.remoteMeta[provider]
	return m.Epoch, m.Version, ok
}
func (sr *ServiceRegistry) ResetRemoteMeta(provider string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	delete(sr.remoteMeta, provider)
}

func (sr *ServiceRegistry) SaveTombMeta(peer string, epoch int64, ver uint64) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.tombMeta[peer] = svcMeta{Epoch: epoch, Version: ver}
}

func (sr *ServiceRegistry) TombMeta(peer string) (int64, uint64, bool) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	tm, ok := sr.tombMeta[peer]
	if !ok {
		return 0, 0, false
	}
	return tm.Epoch, tm.Version, true
}

func (sr *ServiceRegistry) ClearTombMeta(peer string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	delete(sr.tombMeta, peer)
}
