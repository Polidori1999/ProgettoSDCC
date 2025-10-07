package node

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"ProgettoSDCC/pkg/proto"
	"ProgettoSDCC/pkg/services"
)

// Handler autosufficiente: traccia conn, WG, deadline, read → exec → write, cleanup.
func (n *Node) handleRPCConn(c net.Conn) {
	n.rpcWG.Add(1)

	n.rpcMu.Lock()
	n.rpcConns[c] = struct{}{}
	n.rpcMu.Unlock()

	defer func() {
		n.rpcMu.Lock()
		delete(n.rpcConns, c)
		n.rpcMu.Unlock()
		_ = c.Close()
		n.rpcWG.Done()
	}()

	_ = c.SetDeadline(time.Now().Add(5 * time.Second))

	var req proto.InvokeRequest
	if err := proto.ReadJSONFrame(c, &req); err != nil {
		_ = proto.WriteJSONFrame(c, proto.InvokeResponse{ReqID: req.ReqID, OK: false, Error: "bad request"})
		return
	}

	res, err := services.Execute(req.Service, req.A, req.B)
	if err != nil {
		_ = proto.WriteJSONFrame(c, proto.InvokeResponse{ReqID: req.ReqID, OK: false, Error: err.Error()})
		return
	}
	_ = proto.WriteJSONFrame(c, proto.InvokeResponse{
		ReqID:  req.ReqID,
		OK:     true,
		Result: res,
	})
}

// Avvio il listener TCP (se non già attivo) e accetto connessioni, delegando a handleRPCConn.
func (n *Node) ensureRPCServer() {
	n.rpcMu.Lock()
	if n.rpcLn != nil {
		n.rpcMu.Unlock()
		return // già in ascolto
	}
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", n.Port))
	if err != nil {
		n.rpcMu.Unlock()
		log.Printf("[RPC] listen error: %v", err)
		return
	}
	n.rpcLn = ln
	n.rpcMu.Unlock()

	log.Printf("[RPC] listening on :%d", n.Port)

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				// esco quando chiudo il listener allo shutdown / idle
				if strings.Contains(err.Error(), "closed network connection") {
					return
				}
				continue
			}
			go n.handleRPCConn(c)
		}
	}()
}

// Chiudo il listener quando non ho più servizi locali.
func (n *Node) closeRPCServerIfIdle() {
	n.rpcMu.Lock()
	if n.rpcLn != nil {
		_ = n.rpcLn.Close()
		n.rpcLn = nil
	}
	n.rpcMu.Unlock()
}
