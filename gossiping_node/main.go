package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"ProgettoSDCC/pkg/node"
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
	svcFlag := flag.String("services", "", "comma-separated services")
	registryFlag := flag.String("registry", "", "host:port del service registry")
	lookupFlag := flag.String("lookup", "", "service lookup then exit")
	ttlFlag := flag.Int("ttl", 3, "hop-count per lookup")    // (al momento non usato da Node.Run)
	fanoutFlag := flag.Int("fanout", 2, "fanout per lookup") // (al momento non usato da Node.Run)

	// >>> nuovi flag per i parametri RPC
	rpcAFlag := flag.Float64("rpc-a", 18, "Parametro A per l'RPC (float64)")
	rpcBFlag := flag.Float64("rpc-b", 3, "Parametro B per l'RPC (float64)")

	svcCtrlFlag := flag.String("svc-ctrl", "", "file di controllo servizi (comandi: 'ADD <svc>' / 'DEL <svc>')")

	_ = ttlFlag
	_ = fanoutFlag
	flag.Parse()

	if *idFlag == "" {
		h, _ := os.Hostname()
		*idFlag = fmt.Sprintf("%s:%d", h, *portFlag)
	}

	// Bootstrap via registry (se presente)
	var peerList []string
	if *registryFlag != "" {
		if *lookupFlag != "" {
			log.Printf("[INFO] Lookup mode: bootstrap dal registry %s", *registryFlag)
		} else {
			log.Printf("[DBG] Bootstrapping dal registry %s…", *registryFlag)
		}
		peerList = fetchRegistryPeers(*registryFlag, *idFlag)
		log.Printf("[DBG] Peer iniziali dal registry: %v", peerList)
		if len(peerList) == 0 {
			log.Printf("[WARN] Nessun peer restituito dal registry (continuo comunque)")
		}
	}

	// >>> Unico code path: crea il nodo e fai partire Run
	n := node.NewNodeWithID(*idFlag, strings.Join(peerList, ","), *svcFlag)

	n.SetRPCParams(*rpcAFlag, *rpcBFlag)

	if *svcCtrlFlag != "" {
		go watchSvcControlFile(n, *svcCtrlFlag)
	}

	if *lookupFlag != "" {
		log.Printf("[MODE] client lookup=%s", *lookupFlag)
		n.Run(*lookupFlag) // ← QUI fa: lm.Lookup + attesa risposta + shutdown su successo/timeout
		return
	}

	n.Run("") // nodo normale
}

// Commenti in italiano: osserva un file e applica comandi "ADD <svc>" / "DEL <svc>".
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
