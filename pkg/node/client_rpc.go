package node

import (
	"context"
	"fmt"
	"net"
	"time"

	"ProgettoSDCC/pkg/proto"
)

func rpcCall(addr, svc string, a, b float64) (proto.InvokeResponse, error) {
	d := &net.Dialer{Timeout: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	conn, err := d.DialContext(ctx, "tcp", addr)
	cancel()
	if err != nil {
		return proto.InvokeResponse{}, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	req := proto.InvokeRequest{
		ReqID:   fmt.Sprintf("%d", time.Now().UnixNano()),
		Service: svc, A: a, B: b,
	}
	if err := proto.WriteJSONFrame(conn, &req); err != nil {
		return proto.InvokeResponse{}, fmt.Errorf("write: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var resp proto.InvokeResponse
	if err := proto.ReadJSONFrame(conn, &resp); err != nil {
		return proto.InvokeResponse{}, fmt.Errorf("read: %w", err)
	}
	return resp, nil
}
