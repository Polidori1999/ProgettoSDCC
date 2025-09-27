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

	// servizi locali (opzionali)
	svcsCSV := strings.TrimSpace(*servicesFlag)

	// Carica config da .env (niente hardcoded)
	cfg := config.Load()
	log.Printf("[CFG] HB(light=%v, full=%v) SUSPECT=%v DEAD=%v B=%d F=%d T=%d lookup=%s ttl=%d learn=%t repair=%v/%v RPC=(%.2f,%.2f)",
		cfg.HBLightEvery, cfg.HBFullEvery, cfg.SuspectTimeout, cfg.DeadTimeout,
		cfg.FDB, cfg.FDF, cfg.FDT, cfg.LookupMode, cfg.LookupTTL, cfg.LearnFromLookup,
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
	n.SetLookupMode(cfg.LookupMode)
	n.SetLookupTTL(cfg.LookupTTL)
	n.SetLearnFromLookup(cfg.LearnFromLookup)
	n.EnableRepair(cfg.RepairEnabled, cfg.RepairEvery)

	// Parametri RPC per i servizi aritmetici
	n.SetRPCParams(cfg.RPCA, cfg.RPCB)

	// (opzionale) watcher del file comandi servizi
	go watchSvcControlFile(n, "/tmp/services.ctrl")

	// Avvio
	n.Run(lookupSvc)
}

// Osserva un file e applica comandi "ADD <svc>" / "DEL <svc>".
// Semplice polling: appendi righe al file montato nel container.
func watchSvcControlFile(n *node.Node, path string) {
	// assicurati che il file esista
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("[SVCC] errore apertura %s: %v", path, err)
		return
	}
	defer f.Close()

	// log d’avvio
	log.Printf("[SVCC] watching %s (comandi: ADD <svc> | DEL <svc>)", path)

	// posizionati alla fine: processa solo comandi nuovi
	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		log.Printf("[SVCC] seek end fallito: %v", err)
	}

	for {
		r := bufio.NewReader(f)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				break // niente nuove righe al momento
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) != 2 {
				log.Printf("[SVCC] comando non valido: %q", line)
				continue
			}
			cmd := strings.ToUpper(fields[0])
			svc := fields[1]

			switch cmd {
			case "ADD":
				n.AddService(svc)
				log.Printf("[SVCC] ADD %s", svc)
			case "DEL":
				n.RemoveService(svc)
				log.Printf("[SVCC] DEL %s", svc)
			default:
				log.Printf("[SVCC] comando sconosciuto: %q", line)
			}
		}
		time.Sleep(300 * time.Millisecond) // piccolo polling
	}
}
