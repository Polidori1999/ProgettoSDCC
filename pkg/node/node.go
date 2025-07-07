// pkg/node/node.go
package node

import (
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

// Node rappresenta un nodo del cluster gossip
type Node struct {
	ID    string
	Port  int
	Peers map[string]bool
}

// NewNodeWithID crea un nuovo Node usando ID esplicito (host:port)
func NewNodeWithID(id string, peerList string) *Node {
	parts := strings.Split(id, ":")
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Fatalf("invalid port in id: %v", err)
	}
	n := &Node{
		ID:    id,
		Port:  port,
		Peers: make(map[string]bool),
	}
	for _, p := range strings.Split(peerList, ",") {
		if p != "" {
			n.Peers[p] = true
		}
	}
	return n
}

// PeerList restituisce la lista dei peer attivi
func (n *Node) PeerList() []string {
	list := make([]string, 0, len(n.Peers))
	for p := range n.Peers {
		list = append(list, p)
	}
	return list
}

// GossipLoop invia heartbeat UDP a tutti i peer ogni secondo
func (n *Node) GossipLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for peer := range n.Peers {
			go func(p string) {
				addr, err := net.ResolveUDPAddr("udp", p)
				if err != nil {
					return
				}
				conn, err := net.DialUDP("udp", nil, addr)
				if err != nil {
					return
				}
				defer conn.Close()
				msg := []byte("heartbeat:" + n.ID)
				conn.Write(msg)
			}(peer)
		}
	}
}

// FailureDetectorLoop placeholder per la rilevazione di nodi non rispondenti
func (n *Node) FailureDetectorLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		// qui puoi implementare la logica di failure detection
		log.Printf("Current peers: %v\n", n.PeerList())
	}
}

// HandleGossip elabora i messaggi ricevuti
func (n *Node) HandleGossip(msg []byte) {
	// placeholder: log del messaggio
	log.Printf("HandleGossip: %s\n", string(msg))
}
