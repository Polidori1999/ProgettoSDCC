package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"ProgettoSDCC/pkg/node"
)

func main() {
	// 1) Parametri da riga di comando
	idFlag := flag.String("id", "", "node ID host:port (default hostname:port)")
	portFlag := flag.Int("port", 8000, "UDP listen port")
	peersFlag := flag.String("peers", "", "comma-sep peer list")
	svcFlag := flag.String("services", "", "comma-sep services offered")
	lookupFlag := flag.String("lookup", "", "do a service lookup then exit")
	flag.Parse()
	if len(os.Args) == 1 {
		flag.Usage()
		return
	}

	// 2) Costruisci l’ID
	var id string
	if *idFlag != "" {
		id = *idFlag
	} else {
		h, _ := os.Hostname()
		id = h + ":" + strconv.Itoa(*portFlag)
	}

	// 3) Crea il nodo
	n := node.NewNodeWithID(id, *peersFlag, *svcFlag)
	log.Printf("Node %s up – peers=%v services=%v", n.ID, n.PeerList(), n.ServiceTable)

	// 4) Apri connessione UDP
	addr := &net.UDPAddr{IP: net.IPv4zero, Port: *portFlag}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("ListenUDP: %v", err)
	}

	// 5) Avvia gossip & failure detector
	go n.GossipLoop()
	go n.FailureDetectorLoop()

	// 6) Goroutine di lettura UDP stoppabile via `done`
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 4096)
		for {
			select {
			case <-done:
				return
			default:
				nRead, _, err := conn.ReadFromUDP(buf)
				if err != nil {
					// se chiudo la connessione, esco silenziosamente
					if strings.Contains(err.Error(), "closed network connection") {
						return
					}
					log.Printf("ReadUDP: %v", err)
					continue
				}
				n.HandleGossip(buf[:nRead])
			}
		}
	}()

	// 7) Se c’è la lookup, aspetta la convergenza, fai lookup, poi pulisci e esci
	if *lookupFlag != "" {
		log.Printf("Waiting for heartbeats before lookup…")
		time.Sleep(8 * time.Second) // lascia convergere gossip

		if p, ok := n.Lookup(*lookupFlag); ok {
			fmt.Printf("Service %s → %s\n", *lookupFlag, p)
		} else {
			fmt.Printf("Service %s NOT found\n", *lookupFlag)
		}

		// stop reader & chiudi socket
		close(done)
		conn.Close()
		return
	}

	// 8) Nodo “normale”: rimane vivo a girare
	select {}
}
