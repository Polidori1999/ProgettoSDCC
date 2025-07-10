package node

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"ProgettoSDCC/pkg/proto"
)

type Node struct {
	PeerMgr  *PeerManager
	Registry *ServiceRegistry
	Digests  *DigestManager
	GossipM  *GossipManager
	FailureD *FailureDetector
	ID       string
	Port     int

	// rumor tracking for quorum-based failure detector
	suspectCount    map[string]int  // peer → numero di rumor sospetti visti
	seenSuspect     map[string]bool // rumorID sospetti già visti
	seenDead        map[string]bool // rumorID dead già visti
	quorumThreshold int             // soglia di quorum per confermare dead
}

func hasDeadRumorsFor(peer string, seenDead map[string]bool) bool {
	prefix := "dead|" + peer + "|"
	for id := range seenDead {
		if strings.HasPrefix(id, prefix) {
			return true
		}
	}
	return false
}

func NewNodeWithID(id, peerCSV, svcCSV string) *Node {
	parts := strings.Split(id, ":")
	if len(parts) != 2 {
		log.Fatalf("invalid id %s", id)
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Fatalf("bad port: %v", err)
	}

	pm := NewPeerManager(strings.Split(peerCSV, ","), id)
	reg := NewServiceRegistry()
	reg.AddLocal(id, svcCSV)

	dm := NewDigestManager()
	gm := NewGossipManager(pm, dm, reg, id)
	// quorumThreshold fissato a 2; regola in base al numero di peer
	quorum := 2

	// FailureDetector ora accetta gossip manager e due timeout
	fd := NewFailureDetector(pm, reg, gm, 7*time.Second, 10*time.Second)

	n := &Node{
		PeerMgr:  pm,
		Registry: reg,
		Digests:  dm,
		GossipM:  gm,
		FailureD: fd,
		ID:       id,
		Port:     port,

		suspectCount:    make(map[string]int),
		seenSuspect:     make(map[string]bool),
		seenDead:        make(map[string]bool),
		quorumThreshold: quorum,
	}
	return n
}

func (n *Node) Run(lookupSvc string) {
	// 1. Avvia gossip e failure detector
	n.GossipM.Start()
	n.FailureD.Start()

	// 2. Apri socket UDP
	addr := &net.UDPAddr{Port: n.Port, IP: net.IPv4zero}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("ListenUDP: %v", err)
	}
	defer conn.Close()

	// 3. Goroutine di lettura gossip e lookup
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 4096)
		for {
			select {
			case <-done:
				return
			default:
				nRead, srcAddr, err := conn.ReadFromUDP(buf)
				if err != nil {
					if strings.Contains(err.Error(), "closed network connection") {
						return
					}
					log.Printf("ReadUDP: %v", err)
					continue
				}
				env, err := proto.Decode(buf[:nRead])
				if err != nil {
					continue
				}

				switch env.Type {

				case proto.MsgHeartbeat, proto.MsgHeartbeatDigest:
					var hb proto.Heartbeat
					if json.Unmarshal(env.Data, &hb) == nil {
						peer := env.From

						// se era già “morto”, pulisco solo il suo stato rumor
						if n.suspectCount[peer] > 0 || hasDeadRumorsFor(peer, n.seenDead) {
							// cancello tutti i suspectID di quel peer
							for id := range n.seenSuspect {
								if strings.HasPrefix(id, "suspect|"+peer+"|") {
									delete(n.seenSuspect, id)
								}
							}
							// cancello tutti i deadID di quel peer
							for id := range n.seenDead {
								if strings.HasPrefix(id, "dead|"+peer+"|") {
									delete(n.seenDead, id)
								}
							}
							n.suspectCount[peer] = 0
							log.Printf("Peer %s RI-ENTRATO: resetto failure-detector", peer)
						}

						n.PeerMgr.Add(peer)
						n.PeerMgr.Seen(peer)
						n.Registry.Update(peer, hb.Services)
						log.Printf("HB from %-12s services=%v digest=%s", peer, hb.Services, hb.Digest)
					}

				case proto.MsgRumor:
					// TODO: rumor handling

				case proto.MsgLookup:
					lr, err := proto.DecodeLookupRequest(env.Data)
					if err != nil {
						log.Printf("bad LookupRequest: %v", err)
						continue
					}
					log.Printf("RX Lookup %s TTL=%d from %s", lr.Service, lr.TTL, env.From)

					// se ho il servizio, rispondo direttamente
					if provider, ok := n.Registry.Lookup(lr.Service); ok {
						resp := proto.LookupResponse{ID: lr.ID, Provider: provider}
						out, err := proto.Encode(proto.MsgLookupResponse, n.ID, resp)
						if err == nil {
							conn.WriteToUDP(out, srcAddr)
							log.Printf("  -> replied to %s", srcAddr)
						}
						continue
					}

					// forward se TTL>0
					if lr.TTL > 0 {
						lr.TTL--
						out, err := proto.Encode(proto.MsgLookup, n.ID, lr)
						if err == nil {
							for _, p := range n.PeerMgr.List() {
								if p == env.From {
									continue
								}
								n.GossipM.sendUDP(out, p)
							}
							log.Printf("  -> forwarded TTL=%d", lr.TTL)
						}
					}

				case proto.MsgLookupResponse:
					lrsp, err := proto.DecodeLookupResponse(env.Data)
					if err == nil {
						log.Printf("RX LookupResponse %s → %s", lrsp.ID, lrsp.Provider)
					}

				case proto.MsgSuspect:
					// 1) decodifica e dedup
					r, err := proto.DecodeSuspectRumor(env.Data)
					if err != nil || n.seenSuspect[r.RumorID] {
						break
					}
					n.seenSuspect[r.RumorID] = true

					// 2) incremento il conteggio dei sospetti per quel peer
					n.suspectCount[r.Peer]++

					// 3) rilancio sempre il SuspectRumor a tutti i peer
					outS, _ := proto.Encode(proto.MsgSuspect, n.ID, r)
					for _, p := range n.PeerMgr.List() {
						n.GossipM.sendUDP(outS, p)
					}

					// 4) appena raggiungi il quorum (2), logga e genera il DeadRumor
					if n.suspectCount[r.Peer] == n.quorumThreshold {
						log.Printf("Peer %s DEAD (quorum %d raggiunto)", r.Peer, n.quorumThreshold)

						d := proto.DeadRumor{
							RumorID: fmt.Sprintf("dead|%s|%d", r.Peer, time.Now().UnixNano()),
							Peer:    r.Peer,
						}
						outD, _ := proto.Encode(proto.MsgDead, n.ID, d)
						for _, p := range n.PeerMgr.List() {
							n.GossipM.sendUDP(outD, p)
						}
						// rimuovo il provider una sola volta
						n.Registry.RemoveProvider(r.Peer)
					}

				case proto.MsgDead:
					d, err := proto.DecodeDeadRumor(env.Data)
					if err != nil || n.seenDead[d.RumorID] {
						break
					}
					n.seenDead[d.RumorID] = true
					log.Printf("Peer %s DEAD (ricevuto rumor %s)", d.Peer, d.RumorID)
					// rilancio dead rumor
					outD, _ := proto.Encode(proto.MsgDead, n.ID, d)
					for _, p := range n.PeerMgr.List() {
						n.GossipM.sendUDP(outD, p)
					}
					n.Registry.RemoveProvider(d.Peer)

				default:
					log.Printf("unknown msg type %d", env.Type)
				}
			}
		}
	}()

	// 4. Fallback al lookup storico
	if lookupSvc != "" {
		log.Printf("Waiting for heartbeats before lookup…")
		time.Sleep(8 * time.Second)

		if p, ok := n.Registry.Lookup(lookupSvc); ok {
			fmt.Printf("Service %s → %s", lookupSvc, p)
		} else {
			fmt.Printf("Service %s NOT found", lookupSvc)
		}

		close(done)
		conn.Close()
		return
	}

	// 5. Nodo normale: non esce mai
	select {}
}
