package node

import (
	"ProgettoSDCC/pkg/proto"
	"fmt"
	"log"
	"math"
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
	lastDeadTS      map[string]int64 // peer → TS dell’ultimo DEAD visto
	lastLeaveTS     map[string]int64 // peer → TS dell’ultimo LEAVE visto

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
		lastDeadTS:      make(map[string]int64),
		lastLeaveTS:     make(map[string]int64),
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

// node.go
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

func (n *Node) ensureRPCServer() {
	n.rpcMu.Lock()
	defer n.rpcMu.Unlock()
	if n.rpcLn != nil {
		return // già in ascolto
	}
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", n.Port))
	if err != nil {
		log.Printf("[RPC] listen error: %v", err)
		return
	}
	n.rpcLn = ln
	log.Printf("[RPC] listening on :%d", n.Port)
	n.rpcWG.Add(1)
	go func() {
		defer n.rpcWG.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "closed network connection") {
					return
				}
				continue
			}
			n.rpcMu.Lock()
			n.rpcConns[c] = struct{}{}
			n.rpcMu.Unlock()
			n.rpcWG.Add(1)
			go n.handleRPCConn(c) // la tua handler
		}
	}()
}

// conta i peer "alive" basandosi su LastSeen entro failTimeout
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

func (n *Node) closeRPCServerIfIdle() {
	n.rpcMu.Lock()
	if n.rpcLn != nil {
		n.rpcLn.Close()
		n.rpcLn = nil
	}
	n.rpcMu.Unlock()
}

// aggiorna  in base al numero di peer "alive" + nodo locale
func (n *Node) updateQuorum() {
	size := n.alivePeerCount() + 1 // includi me
	n.quorumThreshold = size/2 + 1
}

// restituisce la slice degli indirizzi dei peer "alive"
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
func (n *Node) repairRound() {
	peers := n.alivePeers()
	if len(peers) == 0 {
		return
	}
	k := int(math.Ceil(math.Log2(float64(len(peers)))))
	if k < 1 {
		k = 1
	}
	targets := randomSubset(peers, k, n.GossipM.rnd)

	// Prepara pacchetti (una volta sola)
	full := proto.Heartbeat{
		Services: n.Registry.LocalServices(),
		Peers:    n.PeerMgr.List(), // se vuoi, riduci a 2 hint
	}
	pktFull, _ := proto.Encode(proto.MsgHeartbeat, n.ID, full)

	req := proto.RepairReq{Nonce: time.Now().UnixNano()}
	pktReq, _ := proto.Encode(proto.MsgRepairReq, n.ID, req)

	for _, p := range targets {
		// PUSH: mando il mio stato completo
		n.GossipM.SendUDP(pktFull, p)
		// PULL: chiedo il loro stato completo
		n.GossipM.SendUDP(pktReq, p)
	}
	log.Printf("[REPAIR] push-pull tick: k=%d targets=%v", k, targets)
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

// estrae fino a n peer a caso da peers
func randomSubset(peers []string, k int, rnd *rand.Rand) []string {
	if len(peers) <= k {
		return peers
	}
	out := make([]string, len(peers))
	copy(out, peers)
	rnd.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out[:k]
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
					_, _ = proto.DecodeRepairReq(env.Data)
					resp := proto.Heartbeat{
						Epoch:    n.Registry.LocalEpoch(),
						SvcVer:   n.Registry.LocalVersion(),
						Services: n.Registry.LocalServices(),
						Peers:    n.PeerMgr.List(),
					}
					out, _ := proto.Encode(proto.MsgHeartbeat, n.ID, resp)
					n.GossipM.SendUDP(out, env.From)
					log.Printf("[REPAIR] request from %s → sent HB(full) back", env.From)

				case proto.MsgHeartbeatLight:
					hbd, err := proto.DecodeHeartbeatLight(env.Data)
					if err != nil {
						break
					}
					peer := env.From

					// --- guardia tombstone DEAD ---
					n.rumorMu.RLock()
					tsDead, hadDead := n.lastDeadTS[peer]
					n.rumorMu.RUnlock()
					if hadDead && env.TS <= tsDead {
						break
					}
					if hadDead && env.TS > tsDead {
						n.rumorMu.Lock()
						delete(n.lastDeadTS, peer)
						delete(n.handledDead, peer)
						for id := range n.seenSuspect {
							if strings.HasPrefix(id, "suspect|"+peer+"|") {
								delete(n.seenSuspect, id)
							}
						}
						for id := range n.seenDead {
							if strings.HasPrefix(id, "dead|"+peer+"|") {
								delete(n.seenDead, id)
							}
						}
						n.suspectCount[peer] = 0
						delete(n.seenLeave, peer)
						n.rumorMu.Unlock()
						n.FailureD.UnsuppressPeer(peer)
						log.Printf("Peer %s RI-ENTRATO (dopo DEAD)", peer)
					}

					// --- NEW: guardia tombstone LEAVE ---
					n.rumorMu.RLock()
					tsLeave, hadLeave := n.lastLeaveTS[peer]
					n.rumorMu.RUnlock()
					if hadLeave && env.TS <= tsLeave {
						break
					}
					if hadLeave && env.TS > tsLeave {
						n.rumorMu.Lock()
						delete(n.seenLeave, peer)
						delete(n.lastLeaveTS, peer)
						n.suspectCount[peer] = 0
						// opzionale: pulizia cache rumor del peer
						for id := range n.seenSuspect {
							if strings.HasPrefix(id, "suspect|"+peer+"|") {
								delete(n.seenSuspect, id)
							}
						}
						for id := range n.seenDead {
							if strings.HasPrefix(id, "dead|"+peer+"|") {
								delete(n.seenDead, id)
							}
						}
						n.rumorMu.Unlock()
						n.FailureD.UnsuppressPeer(peer)
						log.Printf("Peer %s RI-ENTRATO (dopo LEAVE)", peer)
					}

					// piggyback peers (non reimportare tombstoned/left)
					for _, p2 := range hbd.Peers {
						if p2 == n.ID {
							continue
						}
						n.rumorMu.RLock()
						tomb := n.handledDead[p2] || n.seenLeave[p2]
						n.rumorMu.RUnlock()
						if !tomb {
							n.PeerMgr.LearnFromPiggyback(p2)
						}
					}

					if e0, v0, ok := n.Registry.RemoteMeta(env.From); !ok || hbd.Epoch > e0 || (hbd.Epoch == e0 && hbd.SvcVer > v0) {
						req := proto.RepairReq{Nonce: time.Now().UnixNano()}
						out, _ := proto.Encode(proto.MsgRepairReq, n.ID, req)
						n.GossipM.SendUDP(out, env.From)
					}
					// vista locale
					n.PeerMgr.Add(peer)
					n.PeerMgr.Seen(peer)
					n.updateQuorum()
					log.Printf("HB(light) from %-12s peers=%v", peer, hbd.Peers)

				case proto.MsgHeartbeat:
					hb, err := proto.DecodeHeartbeat(env.Data)
					if err != nil {
						break
					}
					peer := env.From

					// --- guardia tombstone DEAD ---
					n.rumorMu.RLock()
					tsDead, hadDead := n.lastDeadTS[peer]
					n.rumorMu.RUnlock()
					if hadDead && env.TS <= tsDead {
						break
					}
					if hadDead && env.TS > tsDead {
						n.rumorMu.Lock()
						delete(n.lastDeadTS, peer)
						delete(n.handledDead, peer)
						for id := range n.seenSuspect {
							if strings.HasPrefix(id, "suspect|"+peer+"|") {
								delete(n.seenSuspect, id)
							}
						}
						for id := range n.seenDead {
							if strings.HasPrefix(id, "dead|"+peer+"|") {
								delete(n.seenDead, id)
							}
						}
						n.suspectCount[peer] = 0
						delete(n.seenLeave, peer)
						n.rumorMu.Unlock()
						n.FailureD.UnsuppressPeer(peer)
						log.Printf("Peer %s RI-ENTRATO (dopo DEAD)", peer)
					}

					// --- NEW: guardia tombstone LEAVE ---
					n.rumorMu.RLock()
					tsLeave, hadLeave := n.lastLeaveTS[peer]
					n.rumorMu.RUnlock()
					if hadLeave && env.TS <= tsLeave {
						break
					}
					if hadLeave && env.TS > tsLeave {
						n.rumorMu.Lock()
						delete(n.seenLeave, peer)
						delete(n.lastLeaveTS, peer)
						n.suspectCount[peer] = 0
						for id := range n.seenSuspect {
							if strings.HasPrefix(id, "suspect|"+peer+"|") {
								delete(n.seenSuspect, id)
							}
						}
						for id := range n.seenDead {
							if strings.HasPrefix(id, "dead|"+peer+"|") {
								delete(n.seenDead, id)
							}
						}
						n.rumorMu.Unlock()
						n.FailureD.UnsuppressPeer(peer)
						log.Printf("Peer %s RI-ENTRATO (dopo LEAVE)", peer)
					}

					// piggyback peers
					for _, p2 := range hb.Peers {
						if p2 == n.ID {
							continue
						}
						n.rumorMu.RLock()
						tomb := n.handledDead[p2] || n.seenLeave[p2]
						n.rumorMu.RUnlock()
						if !tomb {
							n.PeerMgr.LearnFromPiggyback(p2)
						}
					}

					// update servizi + vista locale
					n.PeerMgr.Add(peer)
					n.PeerMgr.Seen(peer)
					n.updateQuorum()
					valid := make([]string, 0, len(hb.Services))
					for _, s := range hb.Services {
						s = strings.TrimSpace(strings.ToLower(s))
						if proto.IsValidService(s) {
							valid = append(valid, s)
						}
					}
					if n.Registry.UpdateWithVersion(peer, valid, hb.Epoch, hb.SvcVer) {
						log.Printf("HB(full)   from %-12s epoch=%d ver=%d services=%v peers=%v", peer, hb.Epoch, hb.SvcVer, valid, hb.Peers)
					} else {
						log.Printf("HB(full)   from %-12s ignorato (epoch/ver non nuovi)", peer)
					}

					log.Printf("HB(full)   from %-12s services=%v peers=%v", peer, hb.Services, hb.Peers)

				case proto.MsgLookup:
					lm.HandleRequest(env, srcAddr)
				case proto.MsgLookupResponse:
					lm.HandleResponse(env)
				case proto.MsgSuspect:
					r, err := proto.DecodeSuspectRumor(env.Data)
					if err != nil {
						break
					}

					// rumor tardivo: se ho visto il peer dopo il TS del rumor, ignoro
					tsRumor := time.Unix(0, env.TS)
					if last, ok := n.PeerMgr.GetLastSeen(r.Peer); ok && last.After(tsRumor) {
						log.Printf("ignoro SUSPECT tardivo su %s: rumorTS=%v < lastSeen=%v", r.Peer, tsRumor, last)
						break
					}

					// BLIND-COUNTER: incremento viste; stop oltre F
					n.rumorMu.Lock()
					cnt := n.suspectSeenCnt[r.RumorID] + 1
					n.suspectSeenCnt[r.RumorID] = cnt
					tomb := n.seenLeave[r.Peer] || n.handledDead[r.Peer]
					n.rumorMu.Unlock()
					if tomb {
						break
					}
					if r.MaxFw > 0 && cnt > r.MaxFw {
						break
					}

					// Filtro “fresco”: se per me è fresco, non voto (forward-only)
					if last, ok := n.PeerMgr.GetLastSeen(r.Peer); ok && time.Since(last) < n.FailureD.suspectTimeout {
						// solo forward sotto
					} else if cnt == 1 {
						// APPLY-ONCE: voto solo alla prima vista
						n.rumorMu.Lock()
						n.suspectCount[r.Peer]++
						votes := n.suspectCount[r.Peer]
						need := n.quorumThreshold
						n.rumorMu.Unlock()

						if votes == need && !n.handledDead[r.Peer] {
							n.handledDead[r.Peer] = true
							log.Printf("Peer %s DEAD (quorum %d raggiunto)", r.Peer, need)

							tsNow := time.Now().UnixNano()
							d := proto.DeadRumor{
								RumorID: fmt.Sprintf("dead|%s|%d", r.Peer, tsNow),
								Peer:    r.Peer,
								Fanout:  uint8(n.fdB),
								MaxFw:   uint8(n.fdF),
								TTL:     uint8(n.fdT),
							}
							outD, _ := proto.Encode(proto.MsgDead, n.ID, d)

							// invio GOSSIP ai vivi, escluso il morto
							targets := exclude(n.alivePeers(), r.Peer)
							B := n.fdB
							if B < 1 {
								B = 1
							}
							if B > len(targets) {
								B = len(targets)
							}
							for _, p := range randomSubset(targets, B, n.GossipM.rnd) {
								n.GossipM.SendUDP(outD, p)
							}

							// effetti locali di DEAD
							n.PeerMgr.Remove(r.Peer)
							n.FailureD.SuppressPeer(r.Peer)
							delete(n.suspectCount, r.Peer)
							n.Registry.RemoveProvider(r.Peer)
							n.updateQuorum()
						}
					}

					// FORWARD per-hop (GOSSIP): TTL-- e inoltra a B random (escludi mittente e sospetto)
					if r.TTL > 0 {
						r.TTL--
						outS, _ := proto.Encode(proto.MsgSuspect, n.ID, r)
						peers := exclude(n.PeerMgr.List(), env.From, r.Peer)
						B := int(r.Fanout)
						if B < 1 {
							B = 1
						}
						if B > len(peers) {
							B = len(peers)
						}
						for _, p := range randomSubset(peers, B, n.GossipM.rnd) {
							n.GossipM.SendUDP(outS, p)
						}
						log.Printf("fwd SUSPECT %s TTL=%d B=%d seen=%d/%d", r.Peer, r.TTL, B, cnt, r.MaxFw)
					}

				case proto.MsgDead:
					d, err := proto.DecodeDeadRumor(env.Data)
					if err != nil {
						break
					}

					// rumor tardivo: se ho visto il peer dopo il TS del rumor, ignoro
					tsRumor := time.Unix(0, env.TS)
					if last, ok := n.PeerMgr.GetLastSeen(d.Peer); ok && last.After(tsRumor) {
						log.Printf("ignoro DEAD tardivo su %s: rumorTS=%v < lastSeen=%v", d.Peer, tsRumor, last)
						break
					}

					// BLIND-COUNTER: incremento viste e rispetto F
					n.rumorMu.Lock()
					cnt := n.deadSeenCnt[d.RumorID] + 1
					n.deadSeenCnt[d.RumorID] = cnt

					// se già left o già gestito dead, esci
					if n.seenLeave[d.Peer] || n.handledDead[d.Peer] {
						n.rumorMu.Unlock()
						break
					}

					// salva tombstone TS più recente
					if env.TS > n.lastDeadTS[d.Peer] {
						n.lastDeadTS[d.Peer] = env.TS
					}
					n.rumorMu.Unlock()

					// stop oltre F (solo se F>0 — ma noi lo mettiamo sempre)
					if d.MaxFw > 0 && cnt > d.MaxFw {
						break
					}

					// APPLY-ONCE: effetti solo alla prima vista
					if cnt == 1 {
						log.Printf("DEAD %s — rumor da %s (%s)", d.Peer, env.From, d.RumorID)
						n.PeerMgr.Remove(d.Peer)
						n.FailureD.SuppressPeer(d.Peer)
						delete(n.suspectCount, d.Peer)
						n.Registry.RemoveProvider(d.Peer)
						n.Registry.ResetRemoteMeta(d.Peer)
						n.updateQuorum()
						n.handledDead[d.Peer] = true
					}

					// FORWARD GOSSIP per-hop (TTL--)
					if d.TTL > 0 {
						d.TTL--
						outD, _ := proto.Encode(proto.MsgDead, n.ID, d)

						peers := n.alivePeers() // inoltra tra vivi
						// escludi mittente e peer morto
						filtered := make([]string, 0, len(peers))
						for _, p := range peers {
							if p != env.From && p != d.Peer {
								filtered = append(filtered, p)
							}
						}

						B := int(d.Fanout)
						if B < 1 {
							B = 1
						}
						if B > len(filtered) {
							B = len(filtered)
						}

						for _, p := range randomSubset(filtered, B, n.GossipM.rnd) {
							n.GossipM.SendUDP(outD, p)
						}
						log.Printf("fwd DEAD %s TTL=%d B=%d seen=%d/%d", d.Peer, d.TTL, B, cnt, d.MaxFw)
					}

				case proto.MsgLeave:
					lv, err := proto.DecodeLeave(env.Data)
					if err != nil {
						log.Printf("bad Leave payload: %v", err)
						break
					}
					peer := lv.Peer
					rid := lv.RumorID
					if rid == "" {
						rid = fmt.Sprintf("leave|%s|%d|", peer, env.TS)
					}

					// tardivo
					tsRumor := time.Unix(0, env.TS)
					if last, ok := n.PeerMgr.GetLastSeen(peer); ok && last.After(tsRumor) {
						log.Printf("ignoro LEAVE tardivo su %s", peer)
						break
					}

					// BLIND-COUNTER + apply-once
					n.rumorMu.Lock()
					cnt := n.leaveSeenCnt[rid] + 1
					n.leaveSeenCnt[rid] = cnt
					already := n.seenLeave[peer]
					if !already {
						n.seenLeave[peer] = true
						n.lastLeaveTS[peer] = env.TS
					}
					n.rumorMu.Unlock()
					if lv.MaxFw > 0 && cnt > lv.MaxFw {
						break
					}

					if !already && cnt == 1 {
						n.PeerMgr.Remove(peer)
						n.Registry.RemoveProvider(peer)
						n.updateQuorum()
						log.Printf("Peer %s LEFT (RID=%s)", peer, rid)
					}

					// forward gossip per-hop
					if lv.TTL > 0 {
						lv.TTL--
						fwd := lv
						fwd.RumorID = rid
						raw, _ := proto.Encode(proto.MsgLeave, n.ID, fwd)

						peers := exclude(n.alivePeers(), env.From, peer)
						B := int(lv.Fanout)
						if B < 1 {
							B = 1
						}
						if B > len(peers) {
							B = len(peers)
						}
						for _, p := range randomSubset(peers, B, n.GossipM.rnd) {
							n.GossipM.SendUDP(raw, p)
						}
						log.Printf("fwd LEAVE %s TTL=%d B=%d seen=%d/%d", peer, lv.TTL, B, cnt, lv.MaxFw)
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
