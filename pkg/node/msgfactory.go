package node

import (
	"log"

	"ProgettoSDCC/pkg/proto"
)

// MakeHeartbeatWitigest costruisce un heartbeat con digest usando la nuova API di proto.Encode.
func MakeHeartbeatWithDigest(reg *ServiceRegistry, fromID string, digest string, peers []string) []byte {

	// raccogli servizi locali
	svcs := make([]string, 0, len(reg.Table))
	for s := range reg.Table {
		svcs = append(svcs, s)
	}

	hb := proto.Heartbeat{
		Services: svcs,
		Digest:   digest,
		Peers:    peers, // << aggiungo la lista di peer
	}

	// ora basta chiamare Encode direttamente
	out, err := proto.Encode(proto.MsgHeartbeatDigest, fromID, hb)
	if err != nil {
		log.Fatalf("failed to Encode HeartbeatWithDigest: %v", err)
	}
	return out
}

// MakeHeartbt costruisce un heartbeat “normale” (senza digest esplicito).
func MakeHeartbeat(reg *ServiceRegistry, fromID string, peers []string) []byte {
	svcs := make([]string, 0, len(reg.Table))
	for s := range reg.Table {
		svcs = append(svcs, s)
	}

	hb := proto.Heartbeat{
		Services: svcs,
		Peers:    peers, // << lista peer piggy-back
		// Digest verrà calcolato internamente…
	}

	out, err := proto.Encode(proto.MsgHeartbeat, fromID, hb)

	if err != nil {
		log.Fatalf("failed to Encode Heartbeat: %v", err)
	}
	return out
}
