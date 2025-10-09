package node

import (
	"ProgettoSDCC/pkg/proto"
	"fmt"
	"log"
	"sync"
	"time"
)

// incapsulo i tre parametri del gossip per i rumor (B,F,T).
// Mi serve per passare/leggere set atomici senza spargere singoli int in giro.
type RumorParams struct {
	Fanout      int // B: quanti destinatari scelgo a ogni emissione
	MaxForwards int // F: quante volte è concesso inoltrare un rumor
	TTL         int // T: budget di hop massimo
}

type FailureDetector struct {
	mu    sync.Mutex
	peers *PeerManager // da qui leggo last-seen dei peer (SnapshotLastSeen ecc.)

	gossip         *GossipManager // mi serve per inviare i rumor via UDP
	suspectTimeout time.Duration  // soglia per marcare SUSPECT
	failTimeout    time.Duration  // soglia oltre cui lascio la decisione DEAD al Node (quorum)

	suspected  map[string]bool // peer → ho già gossipato un SuspectRumor per lui (debounce)
	suppressed map[string]bool // peer → non emetto più rumor (dopo DEAD/LEAVE o suppression esplicita)

	stopCh  chan struct{} // canale di stop per la goroutine interna
	fanoutB int           // copia interna dei parametri rumor (in byte-range)
	maxFwF  int
	ttlT    int
}

// mi assicuro che i (B,F,T) stiano in [1,255] perché li serializzo come uint8.
// NB: imposto minimo 1 per evitare "B=0" che non invierebbe mai nulla.
func clampByteRange(x int) int {
	if x < 1 {
		return 1
	}
	if x > 255 {
		return 255
	}
	return x
}

// aggiorno B/F/T in modo thread-safe.
func (fd *FailureDetector) SetRumorParams(p RumorParams) {
	fd.mu.Lock()
	fd.fanoutB = clampByteRange(p.Fanout)
	fd.maxFwF = clampByteRange(p.MaxForwards)
	fd.ttlT = clampByteRange(p.TTL)
	fd.mu.Unlock()
}

// alias per retro-compatibilità con il codice esistente.
func (fd *FailureDetector) SetGossipParams(B, F, T int) {
	fd.SetRumorParams(RumorParams{Fanout: B, MaxForwards: F, TTL: T})
}

// leggo una snapshot dei parametri attuali
func (fd *FailureDetector) Params() RumorParams {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	return RumorParams{fd.fanoutB, fd.maxFwF, fd.ttlT}
}

// costruisco l'FD con mappe inizializzate e timeouts passati dal chiamante.
func NewFailureDetector(pm *PeerManager, gm *GossipManager, suspectT, failT time.Duration) *FailureDetector {
	return &FailureDetector{
		peers:          pm,
		gossip:         gm,
		suspectTimeout: suspectT,
		failTimeout:    failT,
		suspected:      make(map[string]bool),
		suppressed:     make(map[string]bool),
	}
}

// blocco qualsiasi futura segnalazione per il peer (niente più SUSPECT da parte mia).
// Lo uso quando l'ho marcato DEAD (o ha fatto LEAVE) e non voglio flood duplicati.
func (fd *FailureDetector) SuppressPeer(peer string) {
	fd.mu.Lock()
	fd.suppressed[peer] = true
	delete(fd.suspected, peer) // pulisco anche l'eventuale stato SUSPECT
	fd.mu.Unlock()
}

// riabilito un peer (ad es. dopo che l'ho "rivisto" vivo).
// Segno anche "Seen" per azzerare l'età e farlo uscire da eventuali finestre di timeout.
func (fd *FailureDetector) UnsuppressPeer(peer string) {
	fd.mu.Lock()
	delete(fd.suppressed, peer)
	delete(fd.suspected, peer)
	fd.mu.Unlock()
	fd.peers.Seen(peer) // fingo un heartbeat appena ricevuto
}

// emitSuspect: genero e diffondo un SuspectRumor con i parametri correnti (B,F,T).
// Evito di inviare al peer sospetto e faccio anche una self-vote via loopback per
// attivare immediatamente il mio handler (utile a scalare velocemente il quorum).
func (fd *FailureDetector) emitSuspect(peer string, now time.Time) {
	p := fd.Params()

	// Costruisco un rumor-id unico (peer + timestamp) giusto per dedup lato handler.
	rumorID := fmt.Sprintf("suspect|%s|%d", peer, now.UnixNano())
	sr := proto.SuspectRumor{
		RumorID: rumorID,
		Peer:    peer,
		Fanout:  uint8(p.Fanout),
		MaxFw:   uint8(p.MaxForwards),
		TTL:     uint8(p.TTL),
	}
	out, err := proto.Encode(proto.MsgSuspect, fd.gossip.self, sr)
	if err != nil {
		// se non riesco a encodare, non ho altro da fare qui
		return
	}

	// Target: tutti i peer tranne il sospetto.
	all := fd.peers.List()
	filtered := make([]string, 0, len(all))
	for _, x := range all {
		if x != peer {
			filtered = append(filtered, x)
		}
	}

	// Calcolo B effettivo entro i limiti [1, len(filtered)]
	B := p.Fanout
	if B < 1 {
		B = 1
	}
	if B > len(filtered) {
		B = len(filtered)
	}

	// Scelgo un sottoinsieme casuale di B target e invio il rumor.
	for _, to := range randomSubset(filtered, B, fd.gossip.rnd) {
		fd.gossip.SendUDP(out, to)
	}
	log.Printf("[FD] SUSPECT GOSSIP %s B=%d F=%d TTL=%d targets=%v", peer, p.Fanout, p.MaxForwards, p.TTL, filtered)

	// Self-vote: invio il rumor a me stesso per triggherare subito il mio handler.
	fd.gossip.SendUDP(out, fd.gossip.self)
}

// avvio la goroutine periodica (tick 1s) che osserva i last-seen e
// decide quando emettere i SUSPECT. La dichiarazione DEAD non la faccio qui:
// la demanda il  basandosi su quorum (riduce falsi positivi).
func (fd *FailureDetector) Start() {
	fd.mu.Lock()
	if fd.stopCh != nil {
		fd.mu.Unlock()
		return // già avviato
	}
	ch := make(chan struct{})
	fd.stopCh = ch
	fd.mu.Unlock()

	ticker := time.NewTicker(1 * time.Second)

	go func(stop chan struct{}) {
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				// Prendo una snapshot consistente dei last-seen (no lock lungo).
				snap := fd.peers.SnapshotLastSeen()
				for peer, last := range snap {
					// Salto i peer soppressi (già marcati DEAD/LEAVE o temporaneamente esclusi).
					fd.mu.Lock()
					supp := fd.suppressed[peer]
					fd.mu.Unlock()
					if supp {
						continue
					}

					age := now.Sub(last)

					// Caso 1: rientrato entro suspectTimeout → pulisco stato SUSPECT se presente.
					if age <= fd.suspectTimeout {
						fd.mu.Lock()
						if fd.suspected[peer] {
							delete(fd.suspected, peer)
							log.Printf("Peer %s OK (rientrato dal SUSPECT)", peer)
						}
						fd.mu.Unlock()
						continue
					}

					// Caso 2: finestra SUSPECT: (suspectTimeout, failTimeout]
					if age > fd.suspectTimeout && age <= fd.failTimeout {
						fd.mu.Lock()
						first := !fd.suspected[peer] // emetto al primo ingresso in finestra
						if first {
							fd.suspected[peer] = true
						}
						fd.mu.Unlock()

						if first {
							log.Printf("Peer %s SUSPICIOUS (no heartbeat for %v)", peer, age)
							fd.emitSuspect(peer, now)
						}
						continue
					}

				}

			case <-stop:
				return
			}
		}
	}(ch)
}

// fermo la goroutine periodica se è attiva.
func (fd *FailureDetector) Stop() {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	if fd.stopCh != nil {
		close(fd.stopCh)
		fd.stopCh = nil
	}
}

// consento di aggiornare le soglie a runtime (solo valori > 0).

func (fd *FailureDetector) SetTimeouts(suspect, fail time.Duration) {
	if suspect > 0 {
		fd.suspectTimeout = suspect
	}
	if fail > 0 {
		fd.failTimeout = fail
	}
}
