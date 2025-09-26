package node

import (
	"ProgettoSDCC/pkg/proto"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Node struct {
	rumorMu  sync.RWMutex // ← NUOV
	PeerMgr  *PeerManager
	Registry *ServiceRegistry
	GossipM  *GossipManager
	FailureD *FailureDetector
	ID       string
	Port     int

	// rumor tracking for quorum-based failure detector
	suspectCount map[string]int  // peer → numero di rumor sospetti visti
	seenSuspect  map[string]bool // rumorID sospetti già visti
	seenDead     map[string]bool // rumorID dead già visti

	deadSeenCnt     map[string]uint8 // ← NUOVO: rumorID → quante volte l'ho visto
	suspectSeenCnt  map[string]uint8
	leaveSeenCnt    map[string]uint8
	quorumThreshold int             // soglia di quorum per confermare dead
	initialSeeds    []string        // i seed passati in --peers
	seenLeave       map[string]bool // peer → già processato Leave
	handledDead     map[string]bool
	udpConn         *net.UDPConn
	done            chan struct{}
	gossipTicker    *time.Ticker
	shutdownOnce    sync.Once

	//track connessione tcp
	rpcLn    net.Listener
	rpcMu    sync.Mutex
	rpcConns map[net.Conn]struct{}
	rpcWG    sync.WaitGroup

	//parametri servizi
	rpcA float64 // parametro A per l'RPC
	rpcB float64 // parametro B per l'RPC

	repairEnabled bool
	repairEvery   time.Duration
	repairTick    *time.Ticker

	maxTTL int

	lookupTTL       int
	lookupMode      string // "ttl" (default) | "gossip"
	learnFromLookup bool   // se false, pure gossip discovery

	// parametri modalità FD
	fdMode        string // "ttl" | "gossip"
	fdB, fdF, fdT int
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

	seeds := strings.Split(peerCSV, ",")
	pm := NewPeerManager(seeds, id)
	reg := NewServiceRegistry()
	reg.AddLocal(id, svcCSV)
	log.Printf("[BOOT] %s local services: %v", id, reg.LocalServices())

	//dm := NewDigestManager()

	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	gm := NewGossipManager(pm, reg, id, r)

	// quorumThreshold iniziale verrà calcolato da updateQuorum()
	quorum := 0
	// FailureDetector ora accetta gossip manager e due timeout
	fd := NewFailureDetector(pm, reg, gm, 15*time.Second, 22*time.Second)

	n := &Node{
		PeerMgr:  pm,
		Registry: reg,
		//Digests:  dm,
		GossipM:  gm,
		FailureD: fd,
		ID:       id,
		Port:     port,

		suspectCount: make(map[string]int),
		seenSuspect:  make(map[string]bool),
		seenDead:     make(map[string]bool),

		deadSeenCnt:     make(map[string]uint8), // ← init
		handledDead:     make(map[string]bool),
		quorumThreshold: quorum,
		initialSeeds:    seeds,
		seenLeave:       make(map[string]bool),
		done:            make(chan struct{}), // globale cosi non lo
		rpcConns:        make(map[net.Conn]struct{}),
		rpcA:            18, // default sensati, sovrascrivibili dal main
		rpcB:            3,
		lookupTTL:       DefaultLookupTTL,
		maxTTL:          20,
		lookupMode:      "ttl",
		learnFromLookup: true,
		suspectSeenCnt:  make(map[string]uint8),
		leaveSeenCnt:    make(map[string]uint8),

		// parametri gossip FD (usa questi come default fissi)
		fdMode: "gossip", // se non ti interessa più il confronto
		fdB:    3, fdF: 2, fdT: 3,
	}
	// calcola il quorum basato su peer iniziali + me
	fd.SetGossipParams(n.fdB, n.fdF, n.fdT)
	n.updateQuorum()
	return n
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

func (n *Node) SetLookupMode(mode string) {
	switch mode {
	case "ttl", "gossip":
		n.lookupMode = mode
	default:
		n.lookupMode = "ttl"
	}
}
func (n *Node) SetLearnFromLookup(v bool) { n.learnFromLookup = v }

func (n *Node) AddService(svc string) {
	// c’era già almeno un servizio prima di aggiungere?
	had := len(n.Registry.LocalServices()) > 0

	if n.Registry.AddLocalService(n.ID, svc) {
		if !had {
			n.ensureRPCServer() // primo servizio → apri TCP adesso
		}
		n.GossipM.TriggerHeartbeatFullNow()
		log.Printf("[SR] add local service=%q → gossip full now", svc)
	}
}

func (n *Node) RemoveService(svc string) {
	if n.Registry.RemoveLocalService(n.ID, svc) {
		// se ora non ho più servizi locali, chiudo il TCP
		if len(n.Registry.LocalServices()) == 0 {
			n.closeRPCServerIfIdle()
		}
		n.GossipM.TriggerHeartbeatFullNow()
		log.Printf("[SR] remove local service=%q → gossip full now", svc)
	}
}

func (n *Node) EnableRepair(enabled bool, every time.Duration) {
	n.repairEnabled = enabled
	if every <= 0 {
		every = 30 * time.Second
	}
	n.repairEvery = every
}

func (pm *PeerManager) LearnFromPiggyback(peer string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if !pm.Peers[peer] {
		pm.Peers[peer] = true
	}
}

// imposta i parametri A e B usati nelle invocazioni RPC
func (n *Node) SetRPCParams(a, b float64) {
	// Commenti in italiano: operazione semplice, nessuna concorrenza critica qui
	n.rpcA = a
	n.rpcB = b
}

func (n *Node) Shutdown() {
	n.shutdownOnce.Do(func() {
		log.Printf("→ Shutdown: propago Leave e chiudo tutto…")
		rid := fmt.Sprintf("leave|%s|%d|", n.ID, time.Now().UnixNano())
		lv := proto.Leave{
			RumorID: rid, Peer: n.ID,
			Fanout: uint8(n.fdB), MaxFw: uint8(n.fdF), TTL: uint8(n.fdT),
		}
		pkt, _ := proto.Encode(proto.MsgLeave, n.ID, lv)

		alive := n.alivePeers()
		B := n.fdB
		if B < 1 {
			B = 1
		}
		if B > len(alive) {
			B = len(alive)
		}

		for _, p := range randomSubset(alive, B, n.GossipM.rnd) {
			if p != n.ID {
				n.GossipM.SendUDP(pkt, p)
			}
		}
		time.Sleep(500 * time.Millisecond) // breve grace

		if n.repairTick != nil {
			n.repairTick.Stop()
			n.repairTick = nil
		}
		// Ferma componenti periodiche
		if n.GossipM != nil {
			n.GossipM.Stop()
		}
		if n.FailureD != nil {
			n.FailureD.Stop()
		}
		if n.gossipTicker != nil {
			n.gossipTicker.Stop()
		}

		// Segnala done
		close(n.done)

		// Chiudi UDP per sbloccare i reader
		if n.udpConn != nil {
			_ = n.udpConn.Close()
		}

		// === NUOVO: chiudi TCP listener e connessioni attive ===
		if n.rpcLn != nil {
			_ = n.rpcLn.Close() // sblocca Accept
		}
		n.rpcMu.Lock()
		for c := range n.rpcConns {
			_ = c.Close() // sveglia handler bloccati
		}
		n.rpcMu.Unlock()
		// aspetta gli handler (con piccolo timeout)
		doneRPC := make(chan struct{})
		go func() { n.rpcWG.Wait(); close(doneRPC) }()
		select {
		case <-doneRPC:
		case <-time.After(500 * time.Millisecond):
			log.Printf("[RPC] timeout nel wait; proseguo lo shutdown")
		}

		time.Sleep(100 * time.Millisecond)
		log.Printf("← Shutdown completato")
	})
}

func (n *Node) Run(lookupSvc string) {

	// 1. intercetta SIGTERM/SIGINT
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		n.Shutdown()
	}()

	// 1. Avvia gossip e failure detector
	n.GossipM.Start()
	n.FailureD.Start()
	// Avvia il repair push–pull se abilitato
	if n.repairEnabled {
		n.repairTick = time.NewTicker(n.repairEvery)
		go func() {
			for {
				select {
				case <-n.repairTick.C:
					n.repairRound()
				case <-n.done:
					return
				}
			}
		}()
	}
	// avvia RPC TCP se ho servizi locali
	if ls := n.Registry.LocalServices(); len(ls) > 0 {
		go n.serveArithmeticTCP(fmt.Sprintf(":%d", n.Port))
	}

	// 1.b) Log periodico dello stato cluster/quorum
	go func() {
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				peers := n.alivePeers()
				aliveCount := len(peers) + 1
				log.Printf(">> Cluster alive=%d  quorum=%d  peers=%v", aliveCount, n.quorumThreshold, peers)
			case <-n.done:
				return
			}
		}
	}()

	// 2. Apri socket UDP
	addr := &net.UDPAddr{Port: n.Port, IP: net.IPv4zero}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("ListenUDP: %v", err)
	}
	lm := NewLookupManager(n.PeerMgr, n.Registry, n.GossipM, conn)
	lm.SetLearnFromResponses(n.learnFromLookup)

	n.udpConn = conn
	defer conn.Close()

	// 3. Goroutine di lettura gossip e lookup

	go func() {
		buf := make([]byte, 4096)
		for {
			select {
			case <-n.done:
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

				case proto.MsgRepairReq:
					n.handleRepairReq(env)

				case proto.MsgHeartbeatLight:
					if hbd, err := proto.DecodeHeartbeatLight(env.Data); err == nil {
						n.handleHeartbeatLight(env, hbd)
					}

				case proto.MsgHeartbeat:
					if hb, err := proto.DecodeHeartbeat(env.Data); err == nil {
						n.handleHeartbeatFull(env, hb)
					}

				case proto.MsgLookup:
					n.handleLookup(env, lm, srcAddr)

				case proto.MsgLookupResponse:
					n.handleLookupResponse(env, lm)

				case proto.MsgSuspect:
					if r, err := proto.DecodeSuspectRumor(env.Data); err == nil {
						n.handleSuspect(env, r)
					}

				case proto.MsgDead:
					if d, err := proto.DecodeDeadRumor(env.Data); err == nil {
						n.handleDead(env, d)
					}

				case proto.MsgLeave:
					if lv, err := proto.DecodeLeave(env.Data); err == nil {
						n.handleLeave(env, lv)
					}

				default:
					log.Printf("unknown msg type %d", env.Type)
				}
			}
		}
	}()

	// 4. Fallback al lookup storico
	// 4. Lookup client mode
	// 4. Lookup client mode
	if lookupSvc != "" {
		const wait = 8 * time.Second
		deadline := time.Now().Add(wait)

		// piccolo warm-up
		time.Sleep(1200 * time.Millisecond)

		check := time.NewTicker(250 * time.Millisecond)
		defer check.Stop()

		var resend *time.Ticker

		if n.lookupMode == "ttl" {
			// flooding leggero
			resend = time.NewTicker(1 * time.Second)
			defer resend.Stop()
			lm.Lookup(lookupSvc, n.lookupTTL)
		} else {
			// gossip vero: UNA rumor iniziale + (opzionale) una riparazione
			ttl := n.lookupTTL
			if ttl < 2 {
				ttl = 2
			} // 1 hop spesso è poco
			const fanout = 3 // B
			const maxfw = 2  // F

			reqID := lm.LookupGossip(lookupSvc, ttl, n.fdB, n.fdF)
			log.Printf("[LOOKUP] gossip: rumor sent (req=%s TTL=%d B=%d F=%d)", reqID, ttl, n.fdB, n.fdF)

			// (opzionale) unica riparazione dopo ~900ms
			time.AfterFunc(900*time.Millisecond, func() {
				// semplice re-invio con TTL/B incrementati, se vuoi
				// lm.LookupGossip(lookupSvc, ttl+1, fanout+1, maxfw)
			})
		}

		for {
			select {
			case <-check.C:
				if p, ok := n.Registry.Lookup(lookupSvc); ok {
					fmt.Printf("Service %s → %s\n", lookupSvc, p)
					resp, err := rpcCall(p, lookupSvc, n.rpcA, n.rpcB)
					if err != nil {
						fmt.Printf("invoke error: %v\n", err)
					} else if !resp.OK {
						fmt.Printf("invoke error: %s\n", resp.Error)
					} else {
						fmt.Printf("result: %g\n", resp.Result)
					}
					n.Shutdown()
					return
				}
				if time.Now().After(deadline) {
					fmt.Printf("service %s not fount (timeout)\n", lookupSvc)
					n.Shutdown()
					return
				}
			case <-n.done:
				return
			default:
				if resend != nil {
					select {
					case <-resend.C:
						lm.Lookup(lookupSvc, n.lookupTTL)
					default:
					}
				} else {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	}

	// 5. Nodo normale: non esce mai
	<-n.done
	return
}
