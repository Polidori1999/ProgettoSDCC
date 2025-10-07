package node

import (
	"sync"
	"time"
)

// Gestisco la membership e il last-seen dei peer.
type PeerManager struct {
	mu       sync.Mutex
	Peers    map[string]bool      // set dei peer noti
	LastSeen map[string]time.Time // ultimo heartbeat visto per ciascun peer
}

// Inizializzo la struttura partendo da una lista di seed e dal mio ID.
// Segno i seed come vivi "ora" e, soprattutto, evito di inserire me stesso.
func NewPeerManager(initial []string, selfID string) *PeerManager {
	pm := &PeerManager{
		Peers:    make(map[string]bool),
		LastSeen: make(map[string]time.Time),
	}
	now := time.Now()
	for _, p := range initial {
		if p != "" && p != selfID {
			pm.Peers[p] = true
			pm.LastSeen[p] = now
		}
	}
	return pm
}

// Aggiungo/registro un peer (idempotente).
// Aggiorno anche il last-seen così, se lo sto scoprendo adesso, parte "fresco".
func (pm *PeerManager) Add(peer string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.Peers[peer] = true
	pm.LastSeen[peer] = time.Now()
}

// Segno che ho appena visto un heartbeat da peer (non tocco il set).
func (pm *PeerManager) Seen(peer string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.LastSeen[peer] = time.Now()
}

// Rimuovo completamente un peer (usato su DEAD/LEAVE).
func (pm *PeerManager) Remove(peer string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.Peers, peer)
	delete(pm.LastSeen, peer)
}

// Ritorno la lista corrente di peer noti (snapshot in slice).
func (pm *PeerManager) List() []string {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	out := make([]string, 0, len(pm.Peers))
	for p := range pm.Peers {
		out = append(out, p)
	}
	return out
}

// Restituisco il last-seen per un certo peer (se esiste).
func (pm *PeerManager) GetLastSeen(peer string) (time.Time, bool) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	ts, ok := pm.LastSeen[peer]
	return ts, ok
}

// Ritorno una copia *consistente* della mappa LastSeen per scansioni senza lock esterno.
func (pm *PeerManager) SnapshotLastSeen() map[string]time.Time {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	cp := make(map[string]time.Time, len(pm.LastSeen))
	for p, t := range pm.LastSeen {
		cp[p] = t
	}
	return cp
}

// Imparo un peer da piggyback (lo aggiungo al set ma non aggiorno last-seen: non ho HB diretto).
func (pm *PeerManager) LearnFromPiggyback(peer string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if !pm.Peers[peer] {
		pm.Peers[peer] = true
	}
}
