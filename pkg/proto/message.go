package proto

import (
	"encoding/json"
	"time"
)

// ---------- header ----------
type MsgType int

const (
	MsgHeartbeat       MsgType = iota + 1 // 1  – HB “normale”
	MsgHeartbeatDigest                    // 2  – HB con digest
	MsgRumor                              // 3  – blind-counter rumor (prossimo step)
)

type Envelope struct {
	Type MsgType         `json:"type"` // header
	From string          `json:"from"` // nodo mittente "host:port"
	TS   int64           `json:"ts"`   // timestamp (unix-nano)
	Data json.RawMessage `json:"data"` // payload specifico
}

// ---------- payload ----------
type Heartbeat struct {
	Services []string `json:"services,omitempty"` // servizi offerti dal mittente
	Digest   string   `json:"digest,omitempty"`   // hash di peers+servizi
}

type Rumor struct {
	RumorID string `json:"id"`
	Payload []byte `json:"payload"`
}

// ---------- helper ----------
func Encode[T any](mt MsgType, from string, payload T) ([]byte, error) {
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

func DecodeHeartbeat(raw json.RawMessage) (Heartbeat, error) {
	var hb Heartbeat
	err := json.Unmarshal(raw, &hb)
	return hb, err
}

func DecodeRumor(raw json.RawMessage) (Rumor, error) {
	var r Rumor
	err := json.Unmarshal(raw, &r)
	return r, err
}
