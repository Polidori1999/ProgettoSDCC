package proto

import (
	"encoding/json"
	"time"
)

// ---------- header ----------
type MsgType int

const (
	MsgHeartbeat      MsgType = iota + 1 // 1 – HB “full”
	MsgHeartbeatLight                    // 2 – HB “light”
	MsgLookup                            // 3 – lookup request
	MsgLookupResponse                    // 4 – lookup response
	MsgSuspect                           // 5 – rumor “suspect”
	MsgDead                              // 6 – rumor “dead”
	MsgLeave                             // 7 – rumor “leave”
	MsgRepairReq                         // 8 – repair request
)

func (m MsgType) String() string {
	switch m {
	case MsgHeartbeat:
		return "Heartbeat"
	case MsgHeartbeatLight:
		return "HeartbeatLight"
	case MsgLookup:
		return "Lookup"
	case MsgLookupResponse:
		return "LookupResponse"
	case MsgSuspect:
		return "Suspect"
	case MsgDead:
		return "Dead"
	case MsgLeave:
		return "Leave"
	case MsgRepairReq:
		return "RepairReq"
	default:
		return "Unknown"
	}
}

type Envelope struct {
	Type MsgType         `json:"type"`
	From string          `json:"from"`
	TS   int64           `json:"ts"`
	Data json.RawMessage `json:"data"`
}

// ---------- payload ----------
type HeartbeatLight struct {
	Epoch  int64    `json:"epoch"`
	SvcVer uint64   `json:"svcver"`
	Peers  []string `json:"peers,omitempty"`
}

type Heartbeat struct {
	Services []string `json:"services,omitempty"`
	Epoch    int64    `json:"epoch"`
	SvcVer   uint64   `json:"svcver"`
	Peers    []string `json:"peers,omitempty"`
}

type Leave struct {
	RumorID string `json:"rumorID"`
	Peer    string `json:"peer"`
	Fanout  uint8  `json:"fanout,omitempty"`
	MaxFw   uint8  `json:"maxfw,omitempty"`
	TTL     uint8  `json:"ttl,omitempty"`
}

type RepairReq struct {
	Nonce int64 `json:"nonce"`
}

// Lookup
type LookupRequest struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Origin  string `json:"origin"`
	TTL     int    `json:"ttl"`
	Fanout  uint8  `json:"fanout,omitempty"` // B
	MaxFw   uint8  `json:"maxfw,omitempty"`  // F
}

type LookupResponse struct {
	ID       string `json:"id"`
	Provider string `json:"provider"`
}

// Suspect / Dead rumors
type SuspectRumor struct {
	RumorID string `json:"rumorID"`
	Peer    string `json:"peer"`
	Fanout  uint8  `json:"fanout,omitempty"`
	MaxFw   uint8  `json:"maxfw,omitempty"`
	TTL     uint8  `json:"ttl,omitempty"`
}

type DeadRumor struct {
	RumorID string `json:"rumorID"`
	Peer    string `json:"peer"`
	Fanout  uint8  `json:"fanout,omitempty"`
	MaxFw   uint8  `json:"maxfw,omitempty"`
	TTL     uint8  `json:"ttl,omitempty"`
}

// ---------- helpers ----------
func Encode(mt MsgType, from string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	env := Envelope{Type: mt, From: from, TS: time.Now().UnixNano(), Data: raw}
	return json.Marshal(env)
}

func Decode(b []byte) (Envelope, error) {
	var env Envelope
	err := json.Unmarshal(b, &env)
	return env, err
}

// Decoder generico: rimpiazza tutti i DecodeXYZ(...)
func DecodePayload[T any](raw json.RawMessage) (T, error) {
	var v T
	err := json.Unmarshal(raw, &v)
	return v, err
}
