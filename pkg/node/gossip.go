package node

import (
	"ProgettoSDCC/pkg/proto"
	"math/rand"
	"net"
	"time"
)

const MAX int = 5 // numero massimo di "peer hint" che includo negli heartbeat light

// gestisco heartbeat (light/full) e spedizione via UDP ai peer.
type GossipManager struct {
	peers *PeerManager     // da qui leggo la lista dei peer
	reg   *ServiceRegistry // da qui prendo epoch/version/servizi locali
	self  string           // il mio ID (host:port) da mettere nei pacchetti

	// rnd: rng locale per shuffle e scelte casuali (lo ricevo dall'esterno per testabilità)
	rnd *rand.Rand

	// stato di run
	stopCh     chan struct{} // chiusura per fermare la goroutine
	lightT     *time.Ticker  // ticker per HB light
	fullT      *time.Ticker  // ticker per HB full
	hintIdx    int           // indice di rotazione per peerhints se serve
	lightEvery time.Duration // intervallo HB light
	fullEvery  time.Duration // intervallo HB full
}

// costruisco il manager con dipendenze esplicite (peers, registry, self, rng).
func NewGossipManager(pm *PeerManager, reg *ServiceRegistry, selfID string, r *rand.Rand) *GossipManager {
	return &GossipManager{
		peers: pm,
		reg:   reg,
		self:  selfID,
		rnd:   r,
	}
}

// avvio i ticker (light/full) e la goroutine che li ascolta.
func (gm *GossipManager) Start() {
	if gm.stopCh != nil {
		return // già avviato
	}
	// imposto i due intervalli dall'esterno: niente valori hardcoded qui
	if gm.lightEvery <= 0 || gm.fullEvery <= 0 {
		panic("GossipManager: heartbeat intervals not set (call SetHeartbeatIntervals before Start)")
	}

	gm.stopCh = make(chan struct{})
	gm.lightT = time.NewTicker(gm.lightEvery)
	gm.fullT = time.NewTicker(gm.fullEvery)

	go func(stop <-chan struct{}) {
		defer func() {
			if gm.lightT != nil {
				gm.lightT.Stop()
			}
			if gm.fullT != nil {
				gm.fullT.Stop()
			}
		}()
		for {
			select {
			case <-stop:
				return
			case <-gm.lightT.C:
				gm.sendLightHB()
			case <-gm.fullT.C:
				gm.sendFullHB()
			}
		}
	}(gm.stopCh)
}

// invio heartbeat "leggero": epoch/version + pochi hint di peer (no elenco completo, no servizi).
func (gm *GossipManager) sendLightHB() {
	hb := proto.Heartbeat{
		Epoch:  gm.reg.LocalEpoch(),
		SvcVer: gm.reg.LocalVersion(),
		Peers:  gm.peerHints(MAX), // mando solo una finestra rotante di peer come "hint"
	}
	pkt, _ := proto.Encode(proto.MsgHeartbeatLight, gm.self, hb) //
	gm.fanout(pkt)
}

// invio heartbeat "full": epoch/version + servizi locali + lista completa dei peer.
func (gm *GossipManager) sendFullHB() {
	hb := proto.Heartbeat{
		Epoch:    gm.reg.LocalEpoch(),
		SvcVer:   gm.reg.LocalVersion(),
		Services: gm.reg.LocalServices(),
		Peers:    gm.peers.List(),
	}
	pkt, _ := proto.Encode(proto.MsgHeartbeat, gm.self, hb) // TODO: come sopra, valutare log errori di Encode
	gm.fanout(pkt)
}

// fermo la goroutine e i ticker.
func (gm *GossipManager) Stop() {
	if gm.stopCh != nil {
		close(gm.stopCh)
		gm.stopCh = nil
	}
	if gm.lightT != nil {
		gm.lightT.Stop()
		gm.lightT = nil
	}
	if gm.fullT != nil {
		gm.fullT.Stop()
		gm.fullT = nil
	}
}

// scelgo un sottoinsieme di peer casuali (k=logFanout(n)) e invio il pacchetto via UDP.
func (gm *GossipManager) fanout(pkt []byte) {
	peers := gm.peers.List()
	n := len(peers)
	if n == 0 {
		return
	}
	k := logFanout(n) // mi aspetto k>=1; se può tornare 0, devo clampare a 1

	// Shuffle in-place con il mio rng per non sincronizzarmi con altri
	gm.rnd.Shuffle(n, func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	for _, p := range peers[:k] {
		gm.SendUDP(pkt, p)
	}
}

// invio best-effort un datagram al peer; non faccio retry né logging su errori.
func (gm *GossipManager) SendUDP(data []byte, peerAddr string) {
	addr, err := net.ResolveUDPAddr("udp", peerAddr)
	if err != nil {
		return
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return
	}
	conn.Write(data) // ignoro l'errore: heartbeat/rumor sono "fire-and-forget"
	conn.Close()
}

// utility per forzare subito un full-HB (es. dopo ADD/DEL servizio).
func (gm *GossipManager) TriggerHeartbeatFullNow() {
	gm.sendFullHB()
}

// restituisco fino a 'max' peer in modalità round-robin sulla lista corrente.
// Evito di sempre inviare i soliti primi N quando la lista è lunga.
func (gm *GossipManager) peerHints(max int) []string {
	all := gm.peers.List()
	n := len(all)
	if n == 0 || max <= 0 {
		return nil
	}
	if n <= max {
		return all
	}
	start := gm.hintIdx % n
	gm.hintIdx = (gm.hintIdx + max) % n
	out := make([]string, 0, max)
	for i := 0; i < max; i++ {
		out = append(out, all[(start+i)%n])
	}
	return out
}

// imposto gli intervalli (devo chiamarlo prima di Start).
func (g *GossipManager) SetHeartbeatIntervals(light, full time.Duration) {
	if light > 0 {
		g.lightEvery = light
	}
	if full > 0 {
		g.fullEvery = full
	}
}
