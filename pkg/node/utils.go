package node

import (
	"math"
	"math/rand"
)

// Commenti in italiano: utility condivise per slice di peer.

// restituisce fino a k peer distinti, shuffleando in-place una copia.
func randomSubset(peers []string, k int, rnd *rand.Rand) []string {
	if len(peers) <= k {
		return peers
	}
	out := make([]string, len(peers))
	copy(out, peers)
	rnd.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out[:k]
}

// exclude crea una nuova slice che esclude le entry “bad”.
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

// scelgo fino a B target dai peers; garantisco B>=1 e B<=len(peers).
// se peers è vuoto → ritorna nil.
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

func (n *Node) SetLookupTTL(ttl int) {
	if ttl < 0 {
		ttl = 0
	}
	if ttl > n.maxTTL {
		ttl = n.maxTTL
	}
	n.lookupTTL = ttl
}

func (n *Node) SetLearnFromLookup(v bool) { n.learnFromLookup = v }
func (n *Node) SetLearnFromHB(v bool)     { n.learnFromHB = v }

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
