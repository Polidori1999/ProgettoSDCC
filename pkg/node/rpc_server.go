package node

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"ProgettoSDCC/pkg/proto"
	"ProgettoSDCC/pkg/services"
)

func (n *Node) serveArithmeticTCP(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("[RPC] listen %s: %v", addr, err)
		return
	}
	n.rpcLn = ln
	log.Printf("[RPC] in ascolto su tcp://%s", addr)

	// chiudi il listener quando fai shutdown
	go func() { <-n.done; _ = ln.Close() }()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-n.done:
				return // listener chiuso per shutdown
			default:
				if !errors.Is(err, net.ErrClosed) {
					log.Printf("[RPC] accept: %v", err)
				}
				continue
			}
		}

		// traccia conn
		n.rpcMu.Lock()
		n.rpcConns[conn] = struct{}{}
		n.rpcMu.Unlock()

		n.rpcWG.Add(1)
		go func(c net.Conn) {
			defer n.rpcWG.Done()
			defer func() {
				n.rpcMu.Lock()
				delete(n.rpcConns, c)
				n.rpcMu.Unlock()
				_ = c.Close()
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
			// one-shot → return (defer chiude)
		}(conn)
	}
}

func (n *Node) handleRPCConn(c net.Conn) {
	defer c.Close()
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

// Avvia il listener TCP se non già attivo.
func (n *Node) ensureRPCServer() {
	n.rpcMu.Lock()
	defer n.rpcMu.Unlock()
	if n.rpcLn != nil {
		return
	}
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", n.Port))
	if err != nil {
		log.Printf("[RPC] listen error: %v", err)
		return
	}
	n.rpcLn = ln
	log.Printf("[RPC] listening on :%d", n.Port)

	n.rpcWG.Add(1)
	go func() {
		defer n.rpcWG.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "closed network connection") {
					return
				}
				continue
			}
			n.rpcMu.Lock()
			n.rpcConns[c] = struct{}{}
			n.rpcMu.Unlock()

			n.rpcWG.Add(1)
			go n.handleRPCConn(c) // esistente nel tuo progetto
		}
	}()
}

// Chiude il listener quando non ho più servizi locali.
func (n *Node) closeRPCServerIfIdle() {
	n.rpcMu.Lock()
	if n.rpcLn != nil {
		_ = n.rpcLn.Close()
		n.rpcLn = nil
	}
	n.rpcMu.Unlock()
}
