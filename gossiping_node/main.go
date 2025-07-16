package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"ProgettoSDCC/pkg/node"
	"ProgettoSDCC/pkg/proto"
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
	ttlFlag := flag.Int("ttl", 3, "hop-count per lookup")
	fanoutFlag := flag.Int("fanout", 2, "fanout per lookup")
	flag.Parse()

	if *idFlag == "" {
		h, _ := os.Hostname()
		*idFlag = fmt.Sprintf("%s:%d", h, *portFlag)
	}

	var peerList []string
	if *lookupFlag != "" {
		log.Printf("[INFO] Nodo in modalità lookup, nessuna registrazione al registry")
		peerList = []string{}
	} else if *registryFlag != "" {

		log.Printf("[DBG] Bootstrapping dal registry %s…", *registryFlag)
		peerList = fetchRegistryPeers(*registryFlag, *idFlag)
		log.Printf("[DBG] Peer iniziali dal registry: %v", peerList)
		if len(peerList) == 0 {
			log.Fatalf("Nessun peer restituito dal registry")
		}
	}

	if *lookupFlag != "" {
		doReactiveLookup(*lookupFlag, *idFlag, peerList, *portFlag, *ttlFlag, *fanoutFlag)
		return
	}
	n := node.NewNodeWithID(*idFlag, strings.Join(peerList, ","), *svcFlag) //node

	n.Run("")
}

// randomSubset estrae fino a n peer a caso da peers
func randomSubset(peers []string, n int) []string {
	if len(peers) <= n {
		return peers
	}
	out := make([]string, len(peers))
	copy(out, peers)
	for i := 0; i < n; i++ {
		j := i + rand.Intn(len(out)-i)
		out[i], out[j] = out[j], out[i]
	}
	return out[:n]
}

// doReactiveLookup invia una richiesta on-demand e attende la risposta
func doReactiveLookup(service, selfID string, peers []string, port, ttl, fanout int) {
	laddr := &net.UDPAddr{Port: port, IP: net.IPv4zero}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lookup listen error: %v\n", err)
		return
	}
	defer conn.Close()

	reqID := fmt.Sprintf("%d", time.Now().UnixNano())
	lr := proto.LookupRequest{ID: reqID, Service: service, Origin: selfID, TTL: ttl}
	data, err := proto.Encode(proto.MsgLookup, selfID, lr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "encode lookup error: %v\n", err)
		return
	}

	for _, p := range randomSubset(peers, fanout) {
		addr, _ := net.ResolveUDPAddr("udp", p)
		conn.WriteToUDP(data, addr)
	}

	buf := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	nRead, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		fmt.Printf("Service %s NOT found\n", service)
		return
	}
	env, _ := proto.Decode(buf[:nRead])
	if env.Type == proto.MsgLookupResponse {
		resp, _ := proto.DecodeLookupResponse(env.Data)
		if resp.ID == reqID {
			fmt.Printf("Service %s → %s\n", service, resp.Provider)
			return
		}
	}
	fmt.Printf("Service %s NOT found\n", service)
}
