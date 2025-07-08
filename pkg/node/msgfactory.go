package node

import (
	"crypto/sha1"
	"encoding/hex"
	"sort"
	"strings"

	"ProgettoSDCC/pkg/proto"
)

// ──────────────────────────────────────────────────────────
// -- digest di membership + servizi
// ──────────────────────────────────────────────────────────

func (n *Node) computeDigestUnlocked() string {
	peers := make([]string, 0, len(n.Peers))
	for p := range n.Peers {
		peers = append(peers, p)
	}
	sort.Strings(peers)

	svcs := make([]string, 0, len(n.Services))
	for s := range n.Services {
		svcs = append(svcs, s)
	}
	sort.Strings(svcs)

	sum := sha1.Sum([]byte(strings.Join(append(peers, svcs...), ";")))
	return hex.EncodeToString(sum[:])
}

func (n *Node) computeDigest() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.computeDigestUnlocked()
}

// ──────────────────────────────────────────────────────────
// -- costruzione heartbeat (header + payload)
// ──────────────────────────────────────────────────────────

func (n *Node) heartbeatMessage() []byte {
	n.mu.Lock()
	svcs := make([]string, 0, len(n.Services))
	for s := range n.Services {
		svcs = append(svcs, s)
	}
	digest := n.computeDigestUnlocked()
	n.mu.Unlock()

	hb := proto.Heartbeat{Services: svcs, Digest: digest}
	raw, err := proto.Encode(proto.MsgHeartbeatDigest, n.ID, hb)
	if err != nil {
		// errore di codifica: improduttivo loggare qui dentro il factory
		return nil
	}
	return raw
}
