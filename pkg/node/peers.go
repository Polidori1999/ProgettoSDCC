package node

import (
	"sync"
	"time"
)

// PeerManager gestisce la lista dei peer e il loro LastSeen.
type PeerManager struct {
	mu       sync.Mutex
	Peers    map[string]bool
	LastSeen map[string]time.Time
}

// NewPeerManager inizializza il PeerManager con una lista di indirizzi e il proprio ID.
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

// Add registra un nuovo peer.
func (pm *PeerManager) Add(peer string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.Peers[peer] = true
	pm.LastSeen[peer] = time.Now()
}

// Seen aggiorna il timestamp dell’ultimo heartbeat ricevuto da peer.
func (pm *PeerManager) Seen(peer string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.LastSeen[peer] = time.Now()
}

// Remove elimina un peer (dead).
func (pm *PeerManager) Remove(peer string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.Peers, peer)
	delete(pm.LastSeen, peer)
}

// List restituisce una slice degli indirizzi di tutti i peer conosciuti.
func (pm *PeerManager) List() []string {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	out := make([]string, 0, len(pm.Peers))
	for p := range pm.Peers {
		out = append(out, p)
	}
	return out
}
