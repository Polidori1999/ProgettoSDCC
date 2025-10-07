package node

import (
	"ProgettoSDCC/pkg/config"
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

// node è il “contenitore” del mio peer.
// abbiamo PeerMgr (membership), Registry (servizi), GossipM (HB/UDP), FailureD (FD rumor),
// listener UDP, (eventuale) server RPC TCP, e stato per quorum/rumor.
type Node struct {
	rumorMu sync.RWMutex

	PeerMgr  *PeerManager     // gestione peer e last-seen
	Registry *ServiceRegistry // servizi locali/remoti + tombstones
	GossipM  *GossipManager   // invio HB/rumor via UDP
	FailureD *FailureDetector // emissione SUSPECT + timeouts
	ID       string           // "host:port"
	Port     int              // porta (UDP e TCP RPC)

	// ---- stato rumor/quorum ----
	suspectCount    map[string]int   // peer → numero di SUSPECT "voti" ricevuti
	seenSuspect     map[string]bool  // rumorID SUSPECT già visto (dedup)
	deadSeenCnt     map[string]uint8 // rumorID DEAD → contatore viste (per F)
	suspectSeenCnt  map[string]uint8 // rumorID SUSPECT → contatore viste (per F)
	leaveSeenCnt    map[string]uint8 // rumorID LEAVE → contatore viste (per F)
	quorumThreshold int              // soglia per dichiarare DEAD
	seenLeave       map[string]bool  // peer → ho già processato leave
	handledDead     map[string]bool  // peer → ho già applicato DEAD
	udpConn         *net.UDPConn     // mio socket UDP
	done            chan struct{}    // chiusura globale
	shutdownOnce    sync.Once        // per idempotenza di Shutdown

	// ---- TCP RPC server ----
	rpcLn    net.Listener
	rpcMu    sync.Mutex
	rpcConns map[net.Conn]struct{}
	rpcWG    sync.WaitGroup

	// ---- parametri demo RPC ----
	rpcA float64
	rpcB float64

	// ---- repair push–pull ----
	repairEnabled bool
	repairEvery   time.Duration
	repairTick    *time.Ticker

	// ---- lookup policy ----
	lookupTTL       int
	learnFromLookup bool
	learnFromHB     bool // apprendere servizi dagli HB(full)

	// ---- FD gossip params (B,F,T) ----
	fdB, fdF, fdT int

	// ---- logging periodico e client deadline ----
	tickCluster    time.Duration
	clientDeadline time.Duration
}

// costruisco tutto (PeerMgr/Registry/GossipM/FailureD), setto

func NewNodeWithID(id, peerCSV, svcCSV string) *Node {
	cfg := config.Load()

	// Estraggo la porta da id (formato atteso "host:port")
	parts := strings.Split(id, ":")
	if len(parts) != 2 {
		log.Fatalf("invalid id %s", id)
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Fatalf("bad port: %v", err)
	}

	// Seeds iniziali per il PeerManager (possono essere vuoti)
	seeds := strings.Split(peerCSV, ",")
	pm := NewPeerManager(seeds, id)

	// Registro servizi locali (CSV, può essere vuoto)
	reg := NewServiceRegistry()
	reg.AddLocal(id, svcCSV)
	log.Printf("[BOOT] %s local services: %v", id, reg.LocalServices())

	// RNG per GossipManager (seed adesso)
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	gm := NewGossipManager(pm, reg, id, r)

	// FailureDetector: prendo i timeout dalla config
	quorum := 0 // lo ricalcolo subito dopo con updateQuorum()
	fd := NewFailureDetector(pm, gm, cfg.SuspectTimeout, cfg.DeadTimeout)

	n := &Node{
		PeerMgr:  pm,
		Registry: reg,
		GossipM:  gm,
		FailureD: fd,
		ID:       id,
		Port:     port,

		suspectCount: make(map[string]int),
		seenSuspect:  make(map[string]bool),

		deadSeenCnt:     make(map[string]uint8),
		handledDead:     make(map[string]bool),
		quorumThreshold: quorum,
		seenLeave:       make(map[string]bool),
		done:            make(chan struct{}),
		rpcConns:        make(map[net.Conn]struct{}),

		suspectSeenCnt: make(map[string]uint8),
		leaveSeenCnt:   make(map[string]uint8),

		learnFromHB: true, // default: imparo dai full-HB

		// Nota: questi due li tengo costanti salvo override altrove
		tickCluster:    10 * time.Second,
		clientDeadline: 8 * time.Second,
	}
	// Calcolo soglia quorum in base ai peer iniziali
	n.updateQuorum()
	return n
}

// imposto B,F,T lato Node e inoltro al FailureDetector.
func (n *Node) SetGossipParams(B, F, T int) {
	n.fdB, n.fdF, n.fdT = B, F, T
	if n.FailureD != nil {
		n.FailureD.SetGossipParams(B, F, T)
	}
}

// aggiungo un servizio locale; se è il **primo** servizio, apro il TCP RPC.
// In ogni caso, triggero subito un HB(full) per propagare lo stato nuovo.
func (n *Node) AddService(svc string) {
	had := len(n.Registry.LocalServices()) > 0
	if n.Registry.AddLocalService(n.ID, svc) {
		if !had {
			n.ensureRPCServer() // primo servizio → apro listener TCP ora
		}
		n.GossipM.TriggerHeartbeatFullNow()
		log.Printf("[SR] add local service=%q → gossip full now", svc)
	}
}

//	rimuovo un servizio locale; se è l’ultimo, chiudo il TCP RPC.
//
// Poi triggero HB(full) per aggiornare i peer.
func (n *Node) RemoveService(svc string) {
	if n.Registry.RemoveLocalService(n.ID, svc) {
		if len(n.Registry.LocalServices()) == 0 {
			n.closeRPCServerIfIdle()
		}
		n.GossipM.TriggerHeartbeatFullNow()
		log.Printf("[SR] remove local service=%q → gossip full now", svc)
	}
}

// abilito/disabilito il ciclo di repair (con default a 30s se non specificato).
func (n *Node) EnableRepair(enabled bool, every time.Duration) {
	n.repairEnabled = enabled
	if every <= 0 {
		every = 30 * time.Second
	}
	n.repairEvery = every
}

// invio un LEAVE (gossip) a B peer vivi, poi spengo tutto (ticker, goroutine, UDP, TCP).
// Uso shutdownOnce per garantire idempotenza, e chiudo connessioni RPC in modo cooperativo.
func (n *Node) Shutdown() {
	n.shutdownOnce.Do(func() {
		log.Printf("→ Shutdown: propago Leave e chiudo tutto…")

		// Annuncio di LEAVE con rumor-id univoco
		rid := fmt.Sprintf("leave|%s|%d|", n.ID, time.Now().UnixNano())
		lv := proto.Leave{
			RumorID: rid, Peer: n.ID,
			Fanout: uint8(n.fdB), MaxFw: uint8(n.fdF), TTL: uint8(n.fdT),
		}
		pkt, _ := proto.Encode(proto.MsgLeave, n.ID, lv)

		targets := n.pickTargets(exclude(n.alivePeers(), n.ID), n.fdB)
		for _, p := range targets {
			n.GossipM.SendUDP(pkt, p)
		}
		time.Sleep(500 * time.Millisecond) // piccola grace per far circolare il leave

		// Stop repair ticker
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

		// Segnalo done a tutte le goroutine
		close(n.done)

		// Chiudo UDP per sbloccare il reader
		if n.udpConn != nil {
			_ = n.udpConn.Close()
		}

		// Chiudo TCP listener e tutte le connessioni attive
		if n.rpcLn != nil {
			_ = n.rpcLn.Close()
		}
		n.rpcMu.Lock()
		for c := range n.rpcConns {
			_ = c.Close()
		}
		n.rpcMu.Unlock()

		// Aspetto gli handler RPC con timeout breve
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

// avvio segnale di shutdown, componenti periodiche, socket UDP e loop di ricezione.
// Se `lookupSvc` è non vuoto, entro in modalità **client one-shot** (faccio lookup e termino).
func (n *Node) Run(lookupSvc string) {

	// 1) Gestione SIGINT/SIGTERM → Shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		n.Shutdown()
	}()

	// 2) Avvio Gossip e FailureDetector
	n.GossipM.Start()
	n.FailureD.Start()

	// 3) Avvio del repair push–pull (se abilitato)
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

	// 4) Avvio TCP RPC se ho almeno un servizio locale
	if ls := n.Registry.LocalServices(); len(ls) > 0 {
		n.ensureRPCServer()
	}

	// 5) Log periodico di stato cluster/quorum (solo informativo)
	go func() {
		tick := time.NewTicker(n.tickCluster)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				peers := n.alivePeers()
				aliveCount := len(peers) + 1 // includo me stesso
				log.Printf(">> Cluster alive=%d  quorum=%d  peers=%v", aliveCount, n.quorumThreshold, peers)
			case <-n.done:
				return
			}
		}
	}()

	// 6) Apro il socket UDP per ricevere messaggi
	addr := &net.UDPAddr{Port: n.Port, IP: net.IPv4zero}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("ListenUDP: %v", err)
	}
	lm := NewLookupManager(n.PeerMgr, n.Registry, n.GossipM)
	lm.SetLearnFromResponses(n.learnFromLookup)

	n.udpConn = conn
	defer conn.Close()

	// 7) Loop di ricezione (gossip/lookup) su goroutine dedicata
	go func() {
		buf := make([]byte, 4096)
		for {
			select {
			case <-n.done:
				return
			default:
				// Nota: ReadFromUDP è bloccante; lo sblocco con Close() in Shutdown
				nRead, _, err := conn.ReadFromUDP(buf)
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
					if hbd, err := proto.DecodePayload[proto.HeartbeatLight](env.Data); err == nil {
						n.handleHeartbeatLight(env, hbd)
					}

				case proto.MsgHeartbeat:
					if hb, err := proto.DecodePayload[proto.Heartbeat](env.Data); err == nil {
						n.handleHeartbeatFull(env, hb)
					}

				case proto.MsgLookup:
					n.handleLookup(env, lm)

				case proto.MsgLookupResponse:
					n.handleLookupResponse(env, lm)

				case proto.MsgSuspect:
					if r, err := proto.DecodePayload[proto.SuspectRumor](env.Data); err == nil {
						n.handleSuspect(env, r)
					}

				case proto.MsgDead:
					if d, err := proto.DecodePayload[proto.DeadRumor](env.Data); err == nil {
						n.handleDead(env, d)
					}

				case proto.MsgLeave:
					if lv, err := proto.DecodePayload[proto.Leave](env.Data); err == nil {
						n.handleLeave(env, lv)
					}

				default:
					log.Printf("unknown msg type %d", env.Type)
				}
			}
		}
	}()

	// 8) Modalità client one-shot (lookup) → invio gossip lookup, attendo risposta o timeout, poi Shutdown
	if lookupSvc != "" {
		time.Sleep(6 * time.Second) // mini warm-up per scambiare qualche HB
		deadline := time.Now().Add(n.clientDeadline)

		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()

		// TTL iniziale proviene dalla config (settato da main), minimo 2
		initialTTL := n.lookupTTL

		deg := len(n.PeerMgr.List())
		log.Printf("[LOOKUP] warm-up done: degree=%d, B=%d", deg, n.fdB)

		// Invio iniziale (fanout=B, dedupLimit=F)
		reqID := lm.LookupGossip(lookupSvc, initialTTL, n.fdB, n.fdF)
		log.Printf("[LOOKUP] sent (id=%s service=%s initialTTL=%d fanout=%d dedupLimit=%d)",
			reqID, lookupSvc, initialTTL, n.fdB, n.fdF)

		for {
			select {
			case <-tick.C:
				// Se qualcuno ha risposto, il Registry locale ha il mapping
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
					fmt.Printf("service %s not found (timeout)\n", lookupSvc)
					n.Shutdown()
					return
				}
			case <-n.done:
				return
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	// 9) Nodo “server”: resto vivo fino a Shutdown
	<-n.done
	return
}
