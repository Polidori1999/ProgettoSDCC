package node

import "time"

// conto quanti peer considero "alive" guardando il loro last-seen
// entro la finestra del (failTimeout).
func (n *Node) alivePeerCount() int {
	n.PeerMgr.mu.Lock()
	defer n.PeerMgr.mu.Unlock()
	now := time.Now()
	cnt := 0
	for _, ts := range n.PeerMgr.LastSeen {
		if now.Sub(ts) <= n.FailureD.failTimeout {
			cnt++
		}
	}
	return cnt
}

// ricalcolo la soglia come majority di (peer alive + me).
func (n *Node) updateQuorum() {
	size := n.alivePeerCount() + 1 // includo me stesso
	n.quorumThreshold = size/2 + 1
}

// restituisco la lista degli indirizzi "alive" (entro failTimeout).
func (n *Node) alivePeers() []string {
	n.PeerMgr.mu.Lock()
	defer n.PeerMgr.mu.Unlock()
	now := time.Now()
	var out []string
	for peer, ts := range n.PeerMgr.LastSeen {
		if now.Sub(ts) <= n.FailureD.failTimeout {
			out = append(out, peer)
		}
	}
	return out
}
