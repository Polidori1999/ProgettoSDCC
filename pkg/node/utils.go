package node

import "math/rand"

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
