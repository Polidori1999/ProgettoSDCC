package main

import (
	"bufio"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

// salvo un ID progressivo e l'indirizzo host:port del nodo registrato.
type nodeInfo struct {
	id   int
	addr string
}

var (
	listMutex sync.Mutex
	nodeList  []nodeInfo
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("[REG] Nuova connessione da %s", conn.RemoteAddr())

	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		log.Printf("[REG] Errore lettura richiesta: %v", err)
		return
	}
	line = strings.TrimSpace(line)
	log.Printf("[REG] Ricevuto richiesta: %q", line)

	parts := strings.SplitN(line, "#", 2)
	reqID := 0
	addr := ""
	if len(parts) == 2 {
		reqID, _ = strconv.Atoi(parts[0])
		addr = parts[1]
	}

	listMutex.Lock()
	if reqID == 0 {
		reqID = len(nodeList) + 1
		nodeList = append(nodeList, nodeInfo{id: reqID, addr: addr})
	}

	// Rispondi con solo un peer (se disponibile, diverso da quello appena aggiunto)
	var knownPeer string
	for _, n := range nodeList {
		if n.addr != addr {
			knownPeer = strconv.Itoa(n.id) + "/" + n.addr
			break
		}
	}

	resp := strconv.Itoa(reqID)
	if knownPeer != "" {
		resp += "#" + knownPeer
	}
	resp += "\n"
	listMutex.Unlock()

	if _, err := conn.Write([]byte(resp)); err != nil {
		log.Printf("[REG] Errore invio risposta: %v", err)
		return
	}
	log.Printf("[REG] Risposto a %d: %q", reqID, resp)
}

func main() {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("Impossibile ascoltare su :9000: %v", err)
	}
	log.Println("Registry in ascolto su :9000")
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[REG] Accept error: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}
