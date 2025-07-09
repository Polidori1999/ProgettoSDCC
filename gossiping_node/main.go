package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"ProgettoSDCC/pkg/node"
	"ProgettoSDCC/pkg/proto"
)

func main() {
	// 1. Parse flags
	idFlag := flag.String("id", "", "host:port")
	portFlag := flag.Int("port", 8000, "UDP listen port")
	peersFlag := flag.String("peers", "", "comma-sep peer list")
	svcFlag := flag.String("services", "", "comma-sep services")
	lookupFlag := flag.String("lookup", "", "service lookup then exit")
	ttlFlag := flag.Int("ttl", 3, "hop-count (TTL) per lookup on-demand")
	fanoutFlag := flag.Int("fanout", 2, "numero di peer a cui inoltrare il lookup")
	flag.Parse()

	// 2. Default ID se non specificato
	if *idFlag == "" {
		h, _ := os.Hostname()
		*idFlag = fmt.Sprintf("%s:%d", h, *portFlag)
	}

	// 3. Costruzione slice di peer
	peerList := []string{}
	for _, p := range strings.Split(*peersFlag, ",") {
		if p = strings.TrimSpace(p); p != "" {
			peerList = append(peerList, p)
		}
	}

	// 4. Se --lookup, esegui reactive lookup e termina
	if *lookupFlag != "" {
		doReactiveLookup(*lookupFlag, *idFlag, peerList, *portFlag, *ttlFlag, *fanoutFlag)
		return
	}

	// 5. Altrimenti avvia il nodo gossip “classico”
	node := node.NewNodeWithID(*idFlag, *peersFlag, *svcFlag)
	node.Run("")
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
	// 1. apri socket UDP per ricevere risposte
	laddr := &net.UDPAddr{Port: port, IP: net.IPv4zero}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lookup listen error: %v\n", err)
		return
	}
	defer conn.Close()

	// 2. crea il LookupRequest
	reqID := fmt.Sprintf("%d", time.Now().UnixNano())
	lr := proto.LookupRequest{
		ID:      reqID,
		Service: service,
		Origin:  selfID,
		TTL:     ttl,
	}
	data, err := proto.Encode(proto.MsgLookup, selfID, lr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "encode lookup error: %v\n", err)
		return
	}

	// 3. invia a fanout peer casuali
	for _, p := range randomSubset(peers, fanout) {
		addr, _ := net.ResolveUDPAddr("udp", p)
		conn.WriteToUDP(data, addr)
	}

	// 4. aspetta risposta con timeout di 2s
	buf := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		fmt.Printf("Service %s NOT found\n", service)
		return
	}

	// 5. decodifica e stampa
	env, _ := proto.Decode(buf[:n])
	if env.Type == proto.MsgLookupResponse {
		resp, _ := proto.DecodeLookupResponse(env.Data)
		if resp.ID == reqID {
			fmt.Printf("Service %s → %s\n", service, resp.Provider)
			return
		}
	}
	fmt.Printf("Service %s NOT found\n", service)
}
