package node

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"ProgettoSDCC/pkg/proto"
)

const (
	connTimeout = 2 * time.Second // timeout di connessione
	ioTimeout   = 3 * time.Second // timeout complessivo per I/O (read+write)
)

// apro TCP verso addr, invio la richiesta e leggo la risposta in modo semplice.
// Uso un'unica deadline per evitare di restare appeso.
func rpcCall(addr, svc string, a, b float64) (proto.InvokeResponse, error) {
	// Connect con timeout semplice
	conn, err := net.DialTimeout("tcp", addr, connTimeout)
	if err != nil {
		return proto.InvokeResponse{}, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	// Una sola deadline per entrambe le direzioni (write + read)
	_ = conn.SetDeadline(time.Now().Add(ioTimeout))

	// Richiesta (ReqID giusto per tracciare; se non serve, posso rimuoverlo)
	req := proto.InvokeRequest{
		ReqID:   strconv.FormatInt(time.Now().UnixNano(), 10),
		Service: svc,
		A:       a,
		B:       b,
	}

	// Scrivo il frame
	if err := proto.WriteJSONFrame(conn, &req); err != nil {
		return proto.InvokeResponse{}, fmt.Errorf("write: %w", err)
	}

	// Leggo il frame di risposta
	var resp proto.InvokeResponse
	if err := proto.ReadJSONFrame(conn, &resp); err != nil {
		return proto.InvokeResponse{}, fmt.Errorf("read: %w", err)
	}
	return resp, nil
}
