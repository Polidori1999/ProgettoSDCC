package node

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// DigestManager calcola e tiene l’ultimo digest inviato per peer.
type DigestManager struct {
	Last map[string]string // peer → digest
}

// NewDigestManager crea un DigestManager vuoto.
func NewDigestManager() *DigestManager {
	return &DigestManager{Last: make(map[string]string)}
}

// calcola uno sha1 sui contenuti di ServiceRegistry in modo deterministico.
func (dm *DigestManager) Compute(reg *ServiceRegistry) string {
	// 1. ordina servizi
	services := make([]string, 0, len(reg.Table))
	for svc := range reg.Table {
		services = append(services, svc)
	}
	sort.Strings(services)

	// 2. per ciascun servizio, ordina providers
	type entry struct {
		Service  string
		Provider string
		UnixTs   int64
	}
	var all []entry
	for _, svc := range services {
		provs := reg.Table[svc]
		plist := make([]string, 0, len(provs))
		for p := range provs {
			plist = append(plist, p)
		}
		sort.Strings(plist)
		for _, p := range plist {
			all = append(all, entry{svc, p, provs[p].Unix()})
		}
	}

	// 3. serializza e sha1
	b, _ := json.Marshal(all)
	h := sha1.Sum(b)
	return hex.EncodeToString(h[:])
}
