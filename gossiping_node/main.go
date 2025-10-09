package main

import (
	"ProgettoSDCC/pkg/config"
	"ProgettoSDCC/pkg/node"
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

// fetchRegistry: contatto il registry via TCP e provo a farmi dare una lista di peer.
// Ritorno la prima lista non vuota entro maxAttempts; se non trovo nulla, restituisco nil.
func fetchRegistryPeers(regAddr, myID string, maxAttempts int) []string {
	const (
		wait = 1 * time.Second // attendo 1s fra un tentativo e l'altro
	)

	for i := 0; i < maxAttempts; i++ {
		// Apro una connessione TCP verso il registry
		conn, err := net.Dial("tcp", regAddr)
		if err != nil {
			log.Printf("[WARN] Tentativo %d: non riesco a contattare il registry %s: %v", i+1, regAddr, err)
			time.Sleep(wait)
			continue
		}

		// 1) Mi registro inviando il mio ID
		msg := "0#" + myID + "\n"
		log.Printf("[DBG] Richiesta al registry: %q", msg)
		if _, err := conn.Write([]byte(msg)); err != nil {
			log.Printf("[WARN] Errore durante l'invio (tentativo %d): %v", i+1, err)
			conn.Close()
			time.Sleep(wait)
			continue
		}

		// 2) Leggo una riga di risposta e chiudo la connessione
		reply, err := bufio.NewReader(conn).ReadString('\n')
		conn.Close()
		if err != nil {
			log.Printf("[WARN] Errore in lettura dal registry (tentativo %d): %v", i+1, err)
			time.Sleep(wait)
			continue
		}
		reply = strings.TrimSpace(reply)
		log.Printf("[DBG] Risposta del registry: %q", reply)

		// 3) Parso la risposta: splitto per "#", poi ogni segmento è "peerID/host:port"
		parts := strings.Split(reply, "#")

		var peers []string
		for _, p := range parts[1:] { // salto l'eventuale header in parts[0]
			kv := strings.SplitN(p, "/", 2)
			if len(kv) != 2 {
				// formato imprevisto
				continue
			}
			addr := kv[1]
			if addr != myID { // evito di aggiungere me stesso
				peers = append(peers, addr)
			}
		}
		if len(peers) > 0 {
			// ho trovato almeno un peer: mi basta, ritorno subito
			return peers
		}

		// nessun peer per ora: aspetto e ritento
		log.Printf("[DBG] Nessun peer disponibile (tentativo %d). Riprovo tra %v…", i+1, wait)
		time.Sleep(wait)
	}

	// dopo maxAttempts mi arrendo
	log.Printf("[ERR] Nessun peer trovato dopo %d tentativi", maxAttempts)
	return nil
}

func main() {
	// Definisco i flag CLI con default sensati
	idFlag := flag.String("id", "", "host:port")
	portFlag := flag.Int("port", 8000, "UDP listen port")

	registryFlag := flag.String("registry", "", "host:port del service registry")
	lookupFlag := flag.String("lookup", "", "service lookup then exit")

	peersFlag := flag.String("peers", "", "comma-separated initial peers (host:port)")
	servicesFlag := flag.String("services", "", "comma-separated local services")
	svcCtrlFlag := flag.String("svc-ctrl", "", "Path del file di controllo servizi (opzionale)")

	cfg := config.Load()
	log.Printf("[CFG] HB(light=%v, full=%v) hintsMax=%d SUSPECT=%v DEAD=%v B=%d F=%d T=%d ttl=%d learnHB=%t learnLookup=%t repair=%v/%v RPC=(%.2f,%.2f) clusterLog=%v clientDeadline=%v lookupNegTTL=%v registryMaxAttempts=%v",
		cfg.HBLightEvery, cfg.HBFullEvery,
		cfg.HBLightMaxHints,
		cfg.SuspectTimeout, cfg.DeadTimeout,
		cfg.FDB, cfg.FDF, cfg.FDT,
		cfg.LookupTTL, cfg.LearnFromHB, cfg.LearnFromLookup,
		cfg.RepairEnabled, cfg.RepairEvery,
		cfg.RPCA, cfg.RPCB,
		cfg.ClusterLogEvery, cfg.ClientDeadline,
		cfg.LookupNegCacheTTL,
		cfg.RegistryMaxAttempts,
	)
	flag.Parse()

	// Se non mi hanno passato --id, costruisco <hostname>:<port>
	if *idFlag == "" {
		h, _ := os.Hostname() // se fallisce, h sarà vuoto; va comunque bene
		*idFlag = fmt.Sprintf("%s:%d", h, *portFlag)
	}
	id := *idFlag
	lookupSvc := strings.TrimSpace(*lookupFlag)

	// Se ho un registry, provo a fare bootstrap da lì (anche in modalità lookup)
	var peerList []string
	if *registryFlag != "" {
		if lookupSvc != "" {
			log.Printf("[INFO] Modalità lookup: bootstrap dal registry %s", *registryFlag)
		} else {
			log.Printf("[DBG] Faccio bootstrap dal registry %s…", *registryFlag)
		}
		peerList = fetchRegistryPeers(*registryFlag, id, cfg.RegistryMaxAttempts)
		log.Printf("[DBG] Peer iniziali dal registry: %v", peerList)
		if len(peerList) == 0 {
			// Non blocco l'avvio: posso usare comunque --peers o partire solo
			log.Printf("[WARN] Il registry non ha restituito peer (proseguo comunque)")
		}
	}

	// Determino i peer iniziali:
	// - se il registry mi ha dato qualcosa, uso quelli
	// - altrimenti (o se non c'è registry) uso ciò che arriva da --peers
	peersCSV := strings.TrimSpace(*peersFlag)
	if len(peerList) > 0 {
		peersCSV = strings.Join(peerList, ",")
	}

	// Leggo i servizi locali (eventualmente vuoti)
	svcsCSV := strings.TrimSpace(*servicesFlag)

	// Carico la configurazione da .env / variabili d'ambiente (evito costanti hardcoded)

	// Creo il nodo con ID, peers iniziali (CSV) e servizi locali (CSV)
	n := node.NewNodeWithID(id, peersCSV, svcsCSV)

	// Setto i parametri di gossip/rumor (B/F/T) sia per Node che per il FailureDetector
	n.SetGossipParams(cfg.FDB, cfg.FDF, cfg.FDT)

	// Configuro gli intervalli di heartbeat (light/full) nel GossipManager PRIMA del Run
	n.GossipM.SetHeartbeatIntervals(cfg.HBLightEvery, cfg.HBFullEvery)
	n.GossipM.SetLightMaxHints(cfg.HBLightMaxHints)
	// Configuro i timeout del FailureDetector (suspect/dead)
	n.FailureD.SetTimeouts(cfg.SuspectTimeout, cfg.DeadTimeout)

	// Politiche di Lookup e Repair
	n.SetLookupTTL(cfg.LookupTTL)
	n.SetLearnFromLookup(cfg.LearnFromLookup)
	n.EnableRepair(cfg.RepairEnabled, cfg.RepairEvery)
	n.SetLookupNegCacheTTL(cfg.LookupNegCacheTTL)

	// Parametri per l'RPC dei servizi aritmetici + apprendimento dai full-HB
	n.SetRPCParams(cfg.RPCA, cfg.RPCB)
	n.SetLearnFromHB(cfg.LearnFromHB)

	// Avvio il watcher del file di controllo servizi
	ctrlPath := "/tmp/services.ctrl"
	if *svcCtrlFlag != "" {
		ctrlPath = *svcCtrlFlag
	}
	go watchSvcControlFile(n, ctrlPath)

	// Avvio il nodo: se lookupSvc è non vuoto, eseguo una lookup e termino; altrimenti rimango in esecuzione
	n.Run(lookupSvc)
}

// w osservo un file di testo per comandi "ADD <svc>" / "DEL <svc>".
// Leggo solo le righe nuove
func watchSvcControlFile(n *node.Node, path string) {
	// Apro (o creo) il file in sola lettura: mi basta per tail-leggermi i comandi
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("[SVCC] Errore aprendo %s: %v", path, err)
		return
	}
	defer f.Close()
	log.Printf("[SVCC] Watch su %s (comandi: ADD <svc> | DEL <svc>)", path)

	// Mi posiziono in coda: voglio processare solo i comandi scritti dopo l'avvio
	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		log.Printf("[SVCC] Seek a fine file fallito: %v (riparto da inizio)", err)
		off = 0
	}

	var lastLine string // evito di riprocessare la stessa riga se viene riscritta per errore

	for {
		// Se il file è stato troncato/ruotato (size < offset), lo riapro e riparto da fondo
		if st, statErr := f.Stat(); statErr == nil && st.Size() < off {
			_ = f.Close()
			f, err = os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
			if err != nil {
				log.Printf("[SVCC] Riapertura %s fallita: %v", path, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			off, _ = f.Seek(0, io.SeekEnd)
		}

		r := bufio.NewReader(f)
		for {
			// Provo a leggere una nuova riga; se non ce n'è, esco dal loop interno
			line, err := r.ReadString('\n')
			if err != nil {
				break
			}
			off += int64(len(line))

			// Pulisco e filtro righe vuote/commenti o identiche all'ultima già processata
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") || line == lastLine {
				continue
			}
			lastLine = line

			// Mi aspetto esattamente due token: OP e SERVICE
			parts := strings.Fields(line)
			if len(parts) != 2 {
				log.Printf("[SVCC] Comando non valido: %q (atteso: ADD|DEL <svc>)", line)
				continue
			}
			op := strings.ToUpper(parts[0])
			svc := parts[1]

			// Applico il comando al Node
			switch op {
			case "ADD":
				n.AddService(svc) // delego alla logica del Node (annuncio via HB full ecc.)
				log.Printf("[SVCC] ADD %s: OK", svc)
			case "DEL":
				n.RemoveService(svc)
				log.Printf("[SVCC] DEL %s: OK", svc)
			default:
				log.Printf("[SVCC] Operazione sconosciuta: %q", op)
			}
		}

		// Piccolo sleep per non occupare CPU quando il file non cambia
		time.Sleep(250 * time.Millisecond)
	}
}
