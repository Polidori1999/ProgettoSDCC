package node

import (
	"math"
	"math/rand"
)

// ritorno al più k peer distinti.
// Se k >= len(peers) restituisco direttamente la slice (solo lettura!).
func randomSubset(peers []string, k int, rnd *rand.Rand) []string {
	if len(peers) <= k {
		return peers
	}
	out := make([]string, len(peers))
	copy(out, peers)
	rnd.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out[:k]
}

// filtro 'src' rimuovendo gli elementi in 'bad' (lookup O(1) via mappa).
func exclude(src []string, bad ...string) []string {
	if len(src) == 0 {
		return src
	}
	skip := make(map[string]struct{}, len(bad))
	for _, b := range bad {
		skip[b] = struct{}{}
	}
	out := make([]string, 0, len(src))
	for _, s := range src {
		if _, ok := skip[s]; !ok {
			out = append(out, s)
		}
	}
	return out
}

// scelgo fino a B target (B clamped a [1, len(peers)]) usando l’RNG del Gossip.
func (n *Node) pickTargets(peers []string, B int) []string {
	if len(peers) == 0 {
		return nil
	}
	if B < 1 {
		B = 1
	}
	if B > len(peers) {
		B = len(peers)
	}
	return randomSubset(peers, B, n.GossipM.rnd)
}

// clampo il TTL tra 0 e 24 (policy).
func (n *Node) SetLookupTTL(ttl int) {
	if ttl < 0 {
		ttl = 0
	}
	if ttl > 24 {
		ttl = 24
	}
	n.lookupTTL = ttl
}

func (n *Node) SetLearnFromLookup(v bool) { n.learnFromLookup = v }
func (n *Node) SetLearnFromHB(v bool)     { n.learnFromHB = v }

// k  ceil(log2(n)) clampato a [1, n]; se n<=0 → 0.
func logFanout(n int) int {
	if n <= 0 {
		return 0
	}
	k := int(math.Ceil(math.Log2(float64(n))))
	if k < 1 {
		k = 1
	}
	if k > n {
		k = n
	}
	return k
}

// setto i parametri RPC (operazione semplice, niente lock necessario).
func (n *Node) SetRPCParams(a, b float64) {
	n.rpcA = a
	n.rpcB = b
}
