package node

import (
	"encoding/json"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"ProgettoSDCC/pkg/proto"
)

/*───────────────────────────  stato  ───────────────────────────*/

type Node struct {
	ID   string
	Port int

	Services     map[string]bool                 // servizi che offro
	ServiceTable map[string]map[string]time.Time // servizio ⇒ provider ⇒ ts
	Peers        map[string]bool                 // peer conosciuti
	LastSeen     map[string]time.Time            // ultimo HB ricevuto

	mu sync.Mutex
}

/*───────────────────────────  ctor  ───────────────────────────*/

func NewNodeWithID(id, peerCSV, svcCSV string) *Node {
	parts := strings.Split(id, ":")
	if len(parts) != 2 {
		log.Fatalf("invalid id %s (want host:port)", id)
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Fatalf("bad port in id: %v", err)
	}

	n := &Node{
		ID:           id,
		Port:         port,
		Services:     csvToSet(svcCSV),
		ServiceTable: map[string]map[string]time.Time{},
		Peers:        map[string]bool{},
		LastSeen:     map[string]time.Time{},
	}

	now := time.Now()
	for s := range n.Services {
		if n.ServiceTable[s] == nil {
			n.ServiceTable[s] = map[string]time.Time{}
		}
		n.ServiceTable[s][id] = now
	}
	for _, p := range strings.Split(peerCSV, ",") {
		if p = strings.TrimSpace(p); p != "" && p != id {
			n.Peers[p] = true
			n.LastSeen[p] = now
		}
	}
	return n
}

func csvToSet(csv string) map[string]bool {
	m := map[string]bool{}
	for _, x := range strings.Split(csv, ",") {
		if x = strings.TrimSpace(x); x != "" {
			m[x] = true
		}
	}
	return m
}

/*──────────────────────  helper thread-safe  ───────────────────*/

func (n *Node) PeerList() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, 0, len(n.Peers))
	for p := range n.Peers {
		out = append(out, p)
	}
	return out
}

func (n *Node) serviceList() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, 0, len(n.Services))
	for s := range n.Services {
		out = append(out, s)
	}
	return out
}

/*────────────────────────  gossip sender  ──────────────────────*/

func (n *Node) GossipLoop() {
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	for range t.C {
		msg := n.heartbeatMessage() // in msgfactory.go
		for _, p := range n.PeerList() {
			n.sendUDP(msg, p)
		}
	}
}

func (n *Node) sendUDP(data []byte, peer string) {
	addr, err := net.ResolveUDPAddr("udp", peer)
	if err != nil {
		return
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return
	}
	conn.Write(data)
	conn.Close()
}

/*───────────────────────  gossip receiver  ────────────────────*/

func (n *Node) HandleGossip(buf []byte) {
	env, err := proto.Decode(buf)
	if err != nil {
		return
	}
	switch env.Type {

	case proto.MsgHeartbeat, proto.MsgHeartbeatDigest:
		var hb proto.Heartbeat
		if json.Unmarshal(env.Data, &hb) != nil {
			return
		}
		n.handleHeartbeat(env.From, hb)

	case proto.MsgRumor:
		var rm proto.Rumor
		if json.Unmarshal(env.Data, &rm) != nil {
			return
		}
		n.handleRumor(rm)

	default:
		log.Printf("unknown msg type %d", env.Type)
	}
}

func (n *Node) handleHeartbeat(sender string, hb proto.Heartbeat) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.Peers[sender] {
		n.Peers[sender] = true
		log.Printf("Peer %s joined", sender)
	}
	n.LastSeen[sender] = time.Now()

	/* 👇 VERBOSE LOG ad ogni heartbeat */
	log.Printf("HB from %-12s services=%v digest=%s", sender, hb.Services, hb.Digest)

	now := time.Now()
	for _, s := range hb.Services {
		if n.ServiceTable[s] == nil {
			n.ServiceTable[s] = map[string]time.Time{}
		}
		n.ServiceTable[s][sender] = now
	}
}

func (n *Node) handleRumor(r proto.Rumor) {
	log.Printf("received rumor %s (%d bytes)", r.RumorID, len(r.Payload))
}

/*─────────────────────  failure detector  ─────────────────────*/

func (n *Node) FailureDetectorLoop() {
	timeout := 10 * time.Second
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	for range t.C {
		n.mu.Lock()
		now := time.Now()
		for peer, last := range n.LastSeen {
			if now.Sub(last) > timeout {
				delete(n.Peers, peer)
				delete(n.LastSeen, peer)
				for svc, providers := range n.ServiceTable {
					delete(providers, peer)
					if len(providers) == 0 {
						delete(n.ServiceTable, svc)
					}
				}
				log.Printf("Peer %s DEAD", peer)
			}
		}
		n.mu.Unlock()
	}
}

/*──────────────────────────  lookup  ──────────────────────────*/

func (n *Node) Lookup(service string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	prov := n.ServiceTable[service]
	if len(prov) == 0 {
		return "", false
	}
	i := rand.Intn(len(prov))
	j := 0
	for p := range prov {
		if j == i {
			return p, true
		}
		j++
	}
	return "", false
}
