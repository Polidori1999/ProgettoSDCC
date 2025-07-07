// main.go
package main

import (
	"ProgettoSDCC/pkg/node"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)

func main() {
	port := flag.Int("port", 8000, "porta d’ascolto UDP del nodo")
	peers := flag.String("peers", "", "lista peer host:porta separati da virgola")
	flag.Parse()

	if len(os.Args) == 1 {
		fmt.Println("Usage of gossiping_node:")
		flag.PrintDefaults()
		return
	}

	// determina hostname del container (node1, node2, node3)
	host, err := os.Hostname()
	if err != nil {
		log.Fatalf("cannot get hostname: %v", err)
	}
	// costruisci ID come host:port
	id := host + ":" + strconv.Itoa(*port)

	// inizializza il nodo con ID esplicito
	n := node.NewNodeWithID(id, *peers)
	fmt.Printf("Starting node %s with peers %v\n", n.ID, n.PeerList())

	// bind UDP su tutte le interfacce
	addr := &net.UDPAddr{IP: net.IPv4zero, Port: *port}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("ListenUDP: %v", err)
	}
	defer conn.Close()

	// avvia i loop di gossip e failure detector
	go n.GossipLoop()
	go n.FailureDetectorLoop()

	// loop di ricezione UDP
	buf := make([]byte, 4096)
	for {
		nRead, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("ReadFromUDP error: %v\n", err)
			continue
		}
		fmt.Printf("Received %d bytes from %v\n", nRead, remote)
		n.HandleGossip(buf[:nRead])
	}
}
