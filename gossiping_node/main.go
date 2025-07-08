package main

import (
	"flag"
	"fmt"
	"os"
	_ "strconv"

	"ProgettoSDCC/pkg/node"
)

func main() {
	// 1. Parse flags
	idFlag := flag.String("id", "", "host:port")
	portFlag := flag.Int("port", 8000, "UDP listen port")
	peersFlag := flag.String("peers", "", "comma-sep peer list")
	svcFlag := flag.String("services", "", "comma-sep services")
	lookupFlag := flag.String("lookup", "", "service lookup then exit")
	flag.Parse()

	if *idFlag == "" {
		h, _ := os.Hostname()
		*idFlag = fmt.Sprintf("%s:%d", h, *portFlag)
	}

	// 2. Crea e avvia il nodo
	node := node.NewNodeWithID(*idFlag, *peersFlag, *svcFlag)
	node.Run(*lookupFlag)
}
