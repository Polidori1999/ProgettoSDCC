package node

import (
	"ProgettoSDCC/pkg/proto"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// tengo la mappa service  provider lastSeen.
// Inoltre mantengo:
// - localServices: i servizi che io (selfID) sto esponendo ora
// - localEpoch/localVersion: meta locale
// - remoteMeta: meta (epoch,version) che ho visto per ciascun provider remoto
// - tombMeta: meta salvata al momento del DEAD/LEAVE (mi serve per la revival guard)
type ServiceRegistry struct {
	mu            sync.RWMutex
	Table         map[string]map[string]time.Time
	localServices []string
	localEpoch    int64
	localVersion  uint64
	remoteMeta    map[string]svcMeta // provider -> (epoch,ver)
	tombMeta      map[string]svcMeta // peer -> (epoch,ver) al momento della tombstone
}

type svcMeta struct {
	Epoch   int64
	Version uint64
}

func NewServiceRegistry() *ServiceRegistry {
	// epoch lo fisso all’avvio (riavvio ⇒ epoch cambia)
	return &ServiceRegistry{
		Table:      make(map[string]map[string]time.Time),
		localEpoch: time.Now().UnixNano(),
		remoteMeta: make(map[string]svcMeta),
		tombMeta:   make(map[string]svcMeta),
	}
}

// normalizzo il nome servizio (lower + trim) per coerenza ovunque.
func normSvc(s string) string {
	return strings.TrimSpace(strings.ToLower(s))
}

// cerco “svc” nella slice (lineare ma ok, le liste locali sono piccole).
func findSvc(ss []string, svc string) int {
	for i, x := range ss {
		if x == svc {
			return i
		}
	}
	return -1
}

// setto lo stato iniziale dei servizi locali (CSV). La chiamo tipicamente al boot.
// Resetto la slice per evitare duplicati in caso di ri-chiamate.
func (sr *ServiceRegistry) AddLocal(selfID, svcCSV string) {
	now := time.Now()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.localServices = nil // riparto pulito

	for _, raw := range strings.Split(svcCSV, ",") {
		s := normSvc(raw)
		if s == "" || !proto.IsValidService(s) {
			continue
		}
		if sr.Table[s] == nil {
			sr.Table[s] = make(map[string]time.Time)
		}
		sr.Table[s][selfID] = now
		sr.localServices = append(sr.localServices, s)
	}
	// Nota: non tocco localVersion qui: considero AddLocal come “stato iniziale”.
}

// ritorno una copia difensiva (non esporre la slice interna).
func (sr *ServiceRegistry) LocalServices() []string {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	dup := make([]string, len(sr.localServices))
	copy(dup, sr.localServices)
	return dup
}

// apprendo (o rinnovo lastSeen di) servizi remoti da un provider.
// Non tocco remoteMeta: questo è l’update “senza versioning”, usato da lookup o best-effort.
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

// elimino tutte le entry di un provider (dead/leave).
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

// scelgo un provider casuale per “service”. Uso rand globale (thread-safe per i top-level).
func (sr *ServiceRegistry) Lookup(service string) (string, bool) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	provs, ok := sr.Table[service]
	if !ok || len(provs) == 0 {
		return "", false
	}
	// scelta uniforme tra le chiavi della mappa
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

// aggiungo un servizio locale (idempotente).
func (sr *ServiceRegistry) AddLocalService(selfID, svc string) bool {
	svc = normSvc(svc)
	if svc == "" || !proto.IsValidService(svc) {
		return false
	}
	now := time.Now()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	if findSvc(sr.localServices, svc) >= 0 {
		// già presente: rinnovo lastSeen nella tabella e basta
		if sr.Table[svc] == nil {
			sr.Table[svc] = make(map[string]time.Time)
		}
		sr.Table[svc][selfID] = now
		return false
	}

	// vero cambiamento
	sr.localServices = append(sr.localServices, svc)
	if sr.Table[svc] == nil {
		sr.Table[svc] = make(map[string]time.Time)
	}
	sr.Table[svc][selfID] = now
	sr.localVersion++ // bump versione locale solo su change reale
	return true
}

// rimuovo un servizio locale (se c’era) e bumpo la version.
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

	// pulizia tabella
	if provs, ok := sr.Table[svc]; ok {
		delete(provs, selfID)
		if len(provs) == 0 {
			delete(sr.Table, svc)
		}
	}

	sr.localVersion++
	return true
}

// Meta locale
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

// aggiorno lo stato  del provider SOLO se (epoch,ver) è più nuovo.
func (sr *ServiceRegistry) UpdateWithVersion(provider string, services []string, epoch int64, ver uint64) bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	cur := sr.remoteMeta[provider]
	newer := (epoch > cur.Epoch) || (epoch == cur.Epoch && ver > cur.Version)
	if !newer {
		return false
	}

	now := time.Now()
	// ripulisco tutte le entry del provider
	for svc, provs := range sr.Table {
		delete(provs, provider)
		if len(provs) == 0 {
			delete(sr.Table, svc)
		}
	}
	// e riscrivo lo stato “intero”
	for _, s := range services {
		if sr.Table[s] == nil {
			sr.Table[s] = make(map[string]time.Time)
		}
		sr.Table[s][provider] = now
	}
	sr.remoteMeta[provider] = svcMeta{Epoch: epoch, Version: ver}
	return true
}

// Accesso al meta remoto
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

// Tombstones: salvo/leggo/clearo meta al momento di DEAD/LEAVE per la revival guard.
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
