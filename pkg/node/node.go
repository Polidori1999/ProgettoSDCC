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
	fd := NewFailureDetector(pm, reg, 10*time.Second)

	return &Node{
		PeerMgr:  pm,
		Registry: reg,
		Digests:  dm,
		GossipM:  gm,
		FailureD: fd,
		ID:       id,
		Port:     port,
	}
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
				// Catturiamo anche l'indirizzo sorgente
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
						n.PeerMgr.Add(env.From)
						n.PeerMgr.Seen(env.From)
						n.Registry.Update(env.From, hb.Services)
						log.Printf("HB from %-12s services=%v digest=%s",
							env.From, hb.Services, hb.Digest)
					}

				case proto.MsgRumor:
					// TODO: rumor handling

				case proto.MsgLookup:
					// decodifica richiesta
					lr, err := proto.DecodeLookupRequest(env.Data)
					if err != nil {
						log.Printf("bad LookupRequest: %v", err)
						continue
					}
					log.Printf("RX Lookup %s TTL=%d from %s",
						lr.Service, lr.TTL, env.From)

					// se ho il servizio, rispondo direttamente al mittente su srcAddr
					if provider, ok := n.Registry.Lookup(lr.Service); ok {
						resp := proto.LookupResponse{
							ID:       lr.ID,
							Provider: provider,
						}
						out, err := proto.Encode(proto.MsgLookupResponse, n.ID, resp)
						if err == nil {
							// invio la risposta proprio all'indirizzo UDP del client
							conn.WriteToUDP(out, srcAddr)
							log.Printf("  -> replied to %s", srcAddr)
						}
						continue
					}

					// altrimenti forward se TTL>0
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
					// facoltativo: log se qualcun altro riceve la risposta
					lrsp, err := proto.DecodeLookupResponse(env.Data)
					if err == nil {
						log.Printf("RX LookupResponse %s → %s",
							lrsp.ID, lrsp.Provider)
					}

				default:
					log.Printf("unknown msg type %d", env.Type)
				}
			}
		}
	}()

	// 4. Fallback al lookup “storico” se lookupSvc non è vuoto
	if lookupSvc != "" {
		log.Printf("Waiting for heartbeats before lookup…")
		time.Sleep(8 * time.Second)

		if p, ok := n.Registry.Lookup(lookupSvc); ok {
			fmt.Printf("Service %s → %s\n", lookupSvc, p)
		} else {
			fmt.Printf("Service %s NOT found\n", lookupSvc)
		}

		close(done)
		conn.Close()
		return
	}

	// 5. Nodo normale: non esce mai
	select {}
}
