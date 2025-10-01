package main

import (
	"ProgettoSDCC/pkg/config"
	"ProgettoSDCC/pkg/node"
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

// fetchRegistryPeers contatta il registry e restituisce i peer
func fetchRegistryPeers(regAddr, myID string) []string {
	const (
		maxAttempts = 10
		wait        = 1 * time.Second
	)

	for i := 0; i < maxAttempts; i++ {
		conn, err := net.Dial("tcp", regAddr)
		if err != nil {
			log.Printf("[WARN] Tentativo %d: impossibile contattare registry %s: %v", i+1, regAddr, err)
			time.Sleep(wait)
			continue
		}

		// 1) Registrazione
		msg := "0#" + myID + "\n"
		log.Printf("[DBG] Inviata richiesta registry: %q", msg)
		if _, err := conn.Write([]byte(msg)); err != nil {
			log.Printf("[WARN] Errore invio al registry (tentativo %d): %v", i+1, err)
			conn.Close()
			time.Sleep(wait)
			continue
		}

		// 2) Lettura risposta
		reply, err := bufio.NewReader(conn).ReadString('\n')
		conn.Close()
		if err != nil {
			log.Printf("[WARN] Errore lettura dal registry (tentativo %d): %v", i+1, err)
			time.Sleep(wait)
			continue
		}
		reply = strings.TrimSpace(reply)
		log.Printf("[DBG] Risposta registry: %q", reply)

		// 3) Parsing
		parts := strings.Split(reply, "#")

		var peers []string
		for _, p := range parts[1:] {
			kv := strings.SplitN(p, "/", 2)
			if len(kv) != 2 {
				continue
			}
			if addr := kv[1]; addr != myID {
				peers = append(peers, addr)
			}
		}
		if len(peers) > 0 {
			return peers
		}

		log.Printf("[DBG] Nessun peer disponibile (tentativo %d), ritento tra %v…", i+1, wait)
		time.Sleep(wait)
	}

	log.Printf("[ERR] Nessun peer trovato dopo %d tentativi", maxAttempts)
	return nil
}

func main() {
	idFlag := flag.String("id", "", "host:port")
	portFlag := flag.Int("port", 8000, "UDP listen port")

	registryFlag := flag.String("registry", "", "host:port del service registry")
	lookupFlag := flag.String("lookup", "", "service lookup then exit")

	ttlFlag := flag.Int("ttl", 3, "hop-count per lookup (storico)")    // non usato ora
	fanoutFlag := flag.Int("fanout", 2, "fanout per lookup (storico)") // non usato ora

	peersFlag := flag.String("peers", "", "comma-separated initial peers (host:port)")
	servicesFlag := flag.String("services", "", "comma-separated local services")
	svcCtrlFlag := flag.String("svc-ctrl", "", "Path del file di controllo servizi (opzionale)")

	_ = ttlFlag
	_ = fanoutFlag
	flag.Parse()

	// ID di default: <hostname>:<port>
	if *idFlag == "" {
		h, _ := os.Hostname()
		*idFlag = fmt.Sprintf("%s:%d", h, *portFlag)
	}
	id := *idFlag
	lookupSvc := strings.TrimSpace(*lookupFlag)

	// Bootstrap via registry (se presente)
	var peerList []string
	if *registryFlag != "" {
		if lookupSvc != "" {
			log.Printf("[INFO] Lookup mode: bootstrap dal registry %s", *registryFlag)
		} else {
			log.Printf("[DBG] Bootstrapping dal registry %s…", *registryFlag)
		}
		peerList = fetchRegistryPeers(*registryFlag, id)
		log.Printf("[DBG] Peer iniziali dal registry: %v", peerList)
		if len(peerList) == 0 {
			log.Printf("[WARN] Nessun peer restituito dal registry (continuo comunque)")
		}
	}

	// peers iniziali:
	// - se il registry ha dato qualcosa, usiamo quelli
	// - altrimenti (o in aggiunta) usiamo --peers
	peersCSV := strings.TrimSpace(*peersFlag)
	if len(peerList) > 0 {
		peersCSV = strings.Join(peerList, ",")
	}

	// servizi locali
	svcsCSV := strings.TrimSpace(*servicesFlag)

	// Carica config da .env (niente hardcoded)
	cfg := config.Load()
	log.Printf("[CFG] HB(light=%v, full=%v) SUSPECT=%v DEAD=%v B=%d F=%d T=%d ttl=%d learnHB=%t learnLookup=%t repair=%v/%v RPC=(%.2f,%.2f)", cfg.HBLightEvery, cfg.HBFullEvery, cfg.SuspectTimeout, cfg.DeadTimeout,
		cfg.FDB, cfg.FDF, cfg.FDT, cfg.LookupTTL, cfg.LearnFromHB, cfg.LearnFromLookup,
		cfg.RepairEnabled, cfg.RepairEvery, cfg.RPCA, cfg.RPCB)

	// Crea nodo
	n := node.NewNodeWithID(id, peersCSV, svcsCSV)

	// Parametri gossip rumor (B/F/T) → sia Node che FailureDetector
	n.SetGossipParams(cfg.FDB, cfg.FDF, cfg.FDT)

	// Intervalli heartbeat per il GossipManager (devono essere settati prima di Run)
	n.GossipM.SetHeartbeatIntervals(cfg.HBLightEvery, cfg.HBFullEvery)

	// Timeouts del FailureDetector (suspect / dead)
	n.FailureD.SetTimeouts(cfg.SuspectTimeout, cfg.DeadTimeout)

	// Lookup / Repair policy
	n.SetLookupTTL(cfg.LookupTTL)
	n.SetLearnFromLookup(cfg.LearnFromLookup)
	n.EnableRepair(cfg.RepairEnabled, cfg.RepairEvery)

	// Parametri RPC per i servizi aritmetici
	n.SetRPCParams(cfg.RPCA, cfg.RPCB)
	n.SetLearnFromHB(cfg.LearnFromHB)

	ctrlPath := "/tmp/services.ctrl"
	if *svcCtrlFlag != "" {
		ctrlPath = *svcCtrlFlag
	}
	go watchSvcControlFile(n, ctrlPath)

	// Avvio
	n.Run(lookupSvc)
}

// Osserva un file e applica comandi "ADD <svc>" / "DEL <svc>".
// Robusto contro duplicazioni e rotazioni: legge solo nuove righe,
// ignora commenti/righe vuote e gestisce truncation.
func watchSvcControlFile(n *node.Node, path string) {
	// NOTE: commenti in italiano
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("[SVCC] errore apertura %s: %v", path, err)
		return
	}
	defer f.Close()
	log.Printf("[SVCC] watching %s (comandi: ADD <svc> | DEL <svc>)", path)

	// ci posizioniamo in coda per leggere solo comandi futuri
	off, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		log.Printf("[SVCC] seek end fallito: %v", err)
		off = 0
	}

	var lastLine string // evita di riprocessare la stessa riga se viene riscritta per errore

	for {
		// se il file è stato troncato/ruotato, riapri e riparti
		if st, statErr := f.Stat(); statErr == nil && st.Size() < off {
			_ = f.Close()
			f, err = os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
			if err != nil {
				log.Printf("[SVCC] reopen %s: %v", path, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			off, _ = f.Seek(0, os.SEEK_END)
		}

		r := bufio.NewReader(f)
		for {
			// prova a leggere una nuova riga
			line, err := r.ReadString('\n')
			if err != nil {
				// nessuna riga nuova per ora
				break
			}
			off += int64(len(line))

			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") || line == lastLine {
				continue
			}
			lastLine = line

			parts := strings.Fields(line)
			if len(parts) != 2 {
				log.Printf("[SVCC] comando non valido: %q", line)
				continue
			}
			op := strings.ToUpper(parts[0])
			svc := parts[1]

			switch op {
			case "ADD":
				// NB: usa i tuoi metodi: AddService / AddLocalService a seconda di come li hai chiamati
				n.AddService(svc)
				log.Printf("[SVCC] ADD %s ok", svc)
			case "DEL":
				n.RemoveService(svc)
				log.Printf("[SVCC] DEL %s ok", svc)
			default:
				log.Printf("[SVCC] op sconosciuta: %q", op)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
}
