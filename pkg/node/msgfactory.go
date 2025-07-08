package node

import (
	"log"

	"ProgettoSDCC/pkg/proto"
)

// MakeHeartbeatWithDigest costruisce un heartbeat con digest usando la nuova API di proto.Encode.
func MakeHeartbeatWithDigest(reg *ServiceRegistry, fromID string, digest string) []byte {

	// raccogli servizi locali
	svcs := make([]string, 0, len(reg.Table))
	for s := range reg.Table {
		svcs = append(svcs, s)
	}

	hb := proto.Heartbeat{
		Services: svcs,
		Digest:   digest,
	}

	// ora basta chiamare Encode direttamente
	out, err := proto.Encode(proto.MsgHeartbeatDigest, fromID, hb)
	if err != nil {
		log.Fatalf("failed to Encode HeartbeatWithDigest: %v", err)
	}
	return out
}

// MakeHeartbeat costruisce un heartbeat “normale” (senza digest esplicito).
func MakeHeartbeat(reg *ServiceRegistry, fromID string) []byte {
	svcs := make([]string, 0, len(reg.Table))
	for s := range reg.Table {
		svcs = append(svcs, s)
	}

	hb := proto.Heartbeat{
		Services: svcs,
		// Digest verrà calcolato internamente da proto.Encode se necessario,
		// oppure puoi passarlo a mano qui come stringa vuota.
	}

	out, err := proto.Encode(proto.MsgHeartbeat, fromID, hb)

	if err != nil {
		log.Fatalf("failed to Encode Heartbeat: %v", err)
	}
	return out
}
