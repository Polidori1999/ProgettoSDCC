package node

import (
	"ProgettoSDCC/pkg/proto"
	"encoding/json"
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
	initialSeeds    []string        // i seed passati in --peers
	seenLeave       map[string]bool // peer → già processato Leave
	handledDead     map[string]bool
	udpConn         *net.UDPConn
	done            chan struct{}
	gossipTicker    *time.Ticker
	shutdownOnce    sync.Once
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

	seeds := strings.Split(peerCSV, ",")
	pm := NewPeerManager(seeds, id)
	reg := NewServiceRegistry()
	reg.AddLocal(id, svcCSV)

	dm := NewDigestManager()

	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	gm := NewGossipManager(pm, dm, reg, id, r)

	// quorumThreshold iniziale verrà calcolato da updateQuorum()
	quorum := 0
	// FailureDetector ora accetta gossip manager e due timeout
	fd := NewFailureDetector(pm, reg, gm, 15*time.Second, 22*time.Second)

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
		handledDead:     make(map[string]bool),
		quorumThreshold: quorum,
		initialSeeds:    seeds,
		seenLeave:       make(map[string]bool),
		done:            make(chan struct{}), // globale cosi non lo
	}
	// calcola il quorum basato su peer iniziali + me
	n.updateQuorum()
	return n
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

// aggiorna quorumThreshold in base al numero di peer "alive" + nodo locale
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

func (n *Node) hasLeft(peer string) bool {
	n.rumorMu.RLock()
	left := n.seenLeave[peer]
	n.rumorMu.RUnlock()
	return left
}

func (n *Node) markLeft(peer string) {
	n.rumorMu.Lock()
	n.seenLeave[peer] = true
	n.rumorMu.Unlock()
}
func (pm *PeerManager) AddIfNew(peer string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if !pm.Peers[peer] {
		pm.Peers[peer] = true
		pm.LastSeen[peer] = time.Now()
	}
}

func (n *Node) Shutdown() {
	n.shutdownOnce.Do(func() {
		log.Printf("→ Shutdown: propago Leave e chiudo tutto…")

		// 1) invia MsgLeave
		lv := proto.Leave{Peer: n.ID}
		pkt, _ := proto.Encode(proto.MsgLeave, n.ID, lv)
		for _, p := range n.PeerMgr.List() {
			n.GossipM.SendUDP(pkt, p)
		}

		// 2) dai tempo al pacchetto UDP di partire
		time.Sleep(500 * time.Millisecond)

		// 3) ferma componenti periodiche
		if n.gossipTicker != nil {
			n.gossipTicker.Stop()
		}
		if n.FailureD != nil { // NEW: ferma il failure detector
			n.FailureD.Stop()
		}

		// 4) segnala a tutte le goroutine di terminare
		close(n.done)

		// 5) chiudi socket UDP (sblocca ReadFromUDP)
		if n.udpConn != nil {
			n.udpConn.Close()
		}

		// 6) piccola attesa di grazia
		time.Sleep(100 * time.Millisecond)
		log.Printf("← Shutdown completa, exit(0)")
		os.Exit(0)
	})
}

// randomSubset estrae fino a n peer a caso da peers
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

				case proto.MsgHeartbeat, proto.MsgHeartbeatDigest:

					var hb proto.Heartbeat
					if json.Unmarshal(env.Data, &hb) == nil {
						peer := env.From

						for _, p2 := range hb.Peers {
							if p2 != n.ID {
								n.PeerMgr.AddIfNew(p2)
							}
						}

						if n.seenLeave[peer] {
							// era uscito volontariamente, ora sta tornando
							delete(n.seenLeave, peer)
							log.Printf("Peer %s RI-ENTRATO (dopo leave)", peer)
						}
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
							// ← AGGIUNTO: tolgo il flag "già gestito dead"
							delete(n.handledDead, peer)
							// ← AGGIUNTO: riattivo il suo failure-detector
							n.FailureD.UnsuppressPeer(peer)
							log.Printf("Peer %s RI-ENTRATO: resetto failure-detector", peer)

						}
						n.PeerMgr.Add(peer)
						n.updateQuorum()
						n.PeerMgr.Seen(peer)
						n.Registry.Update(peer, hb.Services)
						log.Printf(
							"HB from %-12s services=%v digest=%s peers=%v",
							peer,
							hb.Services,
							hb.Digest,
							hb.Peers, // <<< ora vediamo il tuo piggy-back
						)
					}

				case proto.MsgRumor:
					// TODO: rumor handling

				case proto.MsgLookup:
					lm.HandleRequest(env, srcAddr)
				case proto.MsgLookupResponse:
					lm.HandleResponse(env)
				case proto.MsgSuspect:
					// 1) decode
					r, err := proto.DecodeSuspectRumor(env.Data)
					if err != nil {
						break
					}

					// 1a) rumor "tardivo": se io ho visto il peer DOPO il timestamp del rumor, ignoro
					tsRumor := time.Unix(0, env.TS)
					if last, ok := n.PeerMgr.GetLastSeen(r.Peer); ok && last.After(tsRumor) {
						log.Printf("ignoro SUSPECT tardivo su %s: rumorTS=%v < lastSeen=%v",
							r.Peer, tsRumor, last)
						break
					}

					// 1b) FILTRO LOCALE: se per me il peer è ancora "fresco", NON voto (forward-only una sola volta)
					if last, ok := n.PeerMgr.GetLastSeen(r.Peer); ok {
						age := time.Since(last)
						if age < n.FailureD.suspectTimeout {
							// dedup + skip se già morto/left, tutto in lock
							n.rumorMu.Lock()
							if n.seenSuspect[r.RumorID] || n.seenLeave[r.Peer] || n.handledDead[r.Peer] {
								n.rumorMu.Unlock()
								break
							}
							n.seenSuspect[r.RumorID] = true
							n.rumorMu.Unlock()

							log.Printf("(%s) Ignoro VOTO su %s: rumorID=%s da %s (age=%v < suspectTimeout=%v) — forward only",
								n.ID, r.Peer, r.RumorID, env.From, age, n.FailureD.suspectTimeout)

							// fan-out leggero (k = ceil(log2(n))) escludendo il sospetto
							peers := n.PeerMgr.List()
							if len(peers) > 0 {
								filtered := make([]string, 0, len(peers))
								for _, p := range peers {
									if p != r.Peer {
										filtered = append(filtered, p)
									}
								}
								if len(filtered) > 0 {
									k := int(math.Ceil(math.Log2(float64(len(filtered)))))
									if k < 1 {
										k = 1
									}
									outS, _ := proto.Encode(proto.MsgSuspect, n.ID, r)
									for _, p := range randomSubset(filtered, k, n.GossipM.rnd) {
										n.GossipM.SendUDP(outS, p)
									}
								}
							}
							break // ← non conteggio il voto
						}
					}

					// 2) dedup, mark-as-seen e check Leave **in sezione critica**
					n.rumorMu.Lock()
					if n.seenSuspect[r.RumorID] || n.seenLeave[r.Peer] || n.handledDead[r.Peer] {
						n.rumorMu.Unlock()
						break
					}
					n.seenSuspect[r.RumorID] = true // segno come visto

					if n.seenLeave[r.Peer] { // peer è già LEFT
						n.rumorMu.Unlock()
						break // ignoriamo il suspect
					}

					// aggiorno contatore e prendo snapshot dei votes
					n.suspectCount[r.Peer]++
					votes := n.suspectCount[r.Peer]
					firstRpt := votes == 1
					n.rumorMu.Unlock() // fine sezione critica

					if firstRpt {
						log.Printf("(%s) Peer %s SUSPECT — primo rumor da %s (%d/%d) rumorID=%s",
							n.ID, r.Peer, env.From, votes, n.quorumThreshold, r.RumorID)
					}

					// 3) fan-out parametrico (k = ceil(log2(n))) escludendo il sospetto
					peers := n.PeerMgr.List()
					if len(peers) == 0 {
						break
					}
					filtered := make([]string, 0, len(peers))
					for _, p := range peers {
						if p != r.Peer {
							filtered = append(filtered, p)
						}
					}
					k := 1
					if len(filtered) > 0 {
						k = int(math.Ceil(math.Log2(float64(len(filtered)))))
						if k < 1 {
							k = 1
						}
						outS, _ := proto.Encode(proto.MsgSuspect, n.ID, r)
						for _, p := range randomSubset(filtered, k, n.GossipM.rnd) {
							n.GossipM.SendUDP(outS, p)
						}
					}

					// 4) se abbiamo raggiunto il quorum, generiamo DeadRumor
					if votes == n.quorumThreshold && !n.handledDead[r.Peer] {
						n.handledDead[r.Peer] = true
						log.Printf("Peer %s DEAD (quorum %d raggiunto)", r.Peer, n.quorumThreshold)

						d := proto.DeadRumor{
							RumorID: fmt.Sprintf("dead|%s|%d", r.Peer, time.Now().UnixNano()),
							Peer:    r.Peer,
						}
						n.rumorMu.Lock()
						n.seenDead[d.RumorID] = true
						n.rumorMu.Unlock()

						outD, _ := proto.Encode(proto.MsgDead, n.ID, d)
						// riuso lo stesso k/filtered del fanout sopra
						if len(filtered) > 0 {
							for _, p := range randomSubset(filtered, k, n.GossipM.rnd) {
								n.GossipM.SendUDP(outD, p)
							}
						}
						n.PeerMgr.Remove(r.Peer)
						delete(n.suspectCount, r.Peer)
						n.Registry.RemoveProvider(r.Peer)
						n.updateQuorum()
					}

				case proto.MsgDead:
					d, err := proto.DecodeDeadRumor(env.Data)
					if err != nil {
						break
					}

					tsRumor := time.Unix(0, env.TS)
					if last, ok := n.PeerMgr.GetLastSeen(d.Peer); ok && last.After(tsRumor) {
						log.Printf("ignoro %s tardivo su %s: rumorTS=%v < lastSeen=%v",
							map[proto.MsgType]string{proto.MsgSuspect: "SUSPECT", proto.MsgDead: "DEAD"}[env.Type],
							d.Peer, tsRumor, last)
						break
					}
					// ───────── sezione critica ────────────────────────────────────
					n.rumorMu.Lock()
					if n.seenDead[d.RumorID] || // rumor già visto
						n.seenLeave[d.Peer] || // il peer era uscito volontariamente
						n.handledDead[d.Peer] { // abbiamo già gestito la sua morte
						n.rumorMu.Unlock()
						break
					}

					n.seenDead[d.RumorID] = true // marchiamo il rumor
					n.handledDead[d.Peer] = true // blocchiamo ogni futuro Dead/Suspect
					n.rumorMu.Unlock()
					// ───────────────────────────────────────────────────────────────

					log.Printf("Peer %s DEAD — rumor ricevuto da %s (%s)", d.Peer, env.From, d.RumorID)

					// fan-out parametrico
					alive := n.alivePeers()
					k := int(math.Ceil(math.Log2(float64(len(alive)))))
					if k < 1 {
						k = 1
					}
					outD, _ := proto.Encode(proto.MsgDead, n.ID, d)
					for _, p := range randomSubset(alive, k, n.GossipM.rnd) {
						n.GossipM.SendUDP(outD, p)
					}

					n.PeerMgr.Remove(d.Peer)
					n.FailureD.SuppressPeer(d.Peer)
					delete(n.suspectCount, d.Peer) // ← ripulisci i voti

					n.Registry.RemoveProvider(d.Peer)
					n.updateQuorum()

				case proto.MsgLeave:
					lv, err := proto.DecodeLeave(env.Data)
					if err != nil {
						log.Printf("bad Leave payload: %v", err)
						break
					}
					peer := lv.Peer

					// sezione critica ------------------------------------------------
					n.rumorMu.Lock()
					if n.seenLeave[peer] { // già visto
						n.rumorMu.Unlock()
						break
					}
					n.seenLeave[peer] = true // marchio il leave
					n.rumorMu.Unlock()       // fine sezione critica
					// ----------------------------------------------------------------

					log.Printf("Peer %s → LEFT voluntarily", peer)

					// gossip leggero
					alive := n.alivePeers() // include solo i peer ancora up
					k := int(math.Ceil(math.Log2(float64(len(alive)))))
					if k < 1 {
						k = 1
					}
					raw, _ := proto.Encode(proto.MsgLeave, n.ID, lv)
					for _, p := range randomSubset(alive, k, n.GossipM.rnd) {
						if p == env.From { // evito rinvio al mittente
							continue
						}
						n.GossipM.SendUDP(raw, p)
					}

					n.PeerMgr.Remove(peer)
					n.Registry.RemoveProvider(peer)
					n.updateQuorum()

				default:
					log.Printf("unknown msg type %d", env.Type)
				}
			}
		}
	}()

	// 4. Fallback al lookup storico
	if lookupSvc != "" {
		const wait = 4 * time.Second // tempo massimo di attesa
		deadline := time.Now().Add(wait)
		for time.Now().Before(deadline) {
			if p, ok := n.Registry.Lookup(lookupSvc); ok {
				fmt.Printf("Service %s → %s\n", lookupSvc, p)
				n.Shutdown()
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		fmt.Printf("Service %s NOT found (timeout)\n", lookupSvc)
		n.Shutdown()
		return
	}

	// 5. Nodo normale: non esce mai
	<-n.done
	return
}
