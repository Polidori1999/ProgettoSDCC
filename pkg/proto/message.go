package proto

import (
	"encoding/json"
	"time"
)

// ---------- header ----------
type MsgType int

const (
	MsgHeartbeat      MsgType = iota + 1 // 1  – HB “normale”
	MsgHeartbeatLight                    // 2  – HB con digest
	MsgRumor                             // 3  – blind-counter rumor
	MsgLookup                            // 4  – richiesta di lookup on-demand
	MsgLookupResponse                    // 5  – risposta a una richiesta di lookup
	MsgSuspect                           // 6  – rumor “peer sospetto”
	MsgDead                              // 7  – rumor “peer morto”
	MsgLeave                             // 8 – nodo che si ritira volontariamente
	MsgRepairReq
)

type Envelope struct {
	Type MsgType         `json:"type"`
	From string          `json:"from"`
	TS   int64           `json:"ts"`
	Data json.RawMessage `json:"data"`
}

// ─────────────────────────────────────────────────────────────────────────────
//
//	HeartbeatDigest: versione leggera inviata ogni secondo
//
// ─────────────────────────────────────────────────────────────────────────────
type HeartbeatLight struct {
	Epoch  int64    `json:"epoch"`
	SvcVer uint64   `json:"svcver"`
	Peers  []string `json:"peers,omitempty"` // piggy-back dei peer noti
}

// ---------- payload ----------
type Heartbeat struct {
	// ― versione “full”, usata solo nell’anti-entropy (step 2)
	Services []string `json:"services"` // elenco servizi offerti
	Epoch    int64    `json:"epoch"`
	SvcVer   uint64   `json:"svcver"`          // snapshot SHA-1
	Peers    []string `json:"peers,omitempty"` // piggy-back peer list (facolt.)
}

type Leave struct {
	RumorID string `json:"rumorID"`
	Peer    string `json:"peer"`
	Fanout  uint8  `json:"fanout,omitempty"`
	MaxFw   uint8  `json:"maxfw,omitempty"`
	TTL     uint8  `json:"ttl,omitempty"`
}

// Repair request (payload minimale)
type RepairReq struct {
	Nonce int64 `json:"nonce"`
}

type Rumor struct {
	RumorID string `json:"id"`
	Payload []byte `json:"payload"`
}

// --- lookup on-demand payloads ---
// --- lookup on-demand payloads ---
type LookupRequest struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Origin  string `json:"origin"`
	TTL     int    `json:"ttl"`

	// NUOVI (gossip). Omessi (=0) ⇒ modalità flooding.
	Fanout uint8 `json:"fanout,omitempty"` // B: peer random per hop
	MaxFw  uint8 `json:"maxfw,omitempty"`  // F: quante volte inoltrare la stessa rumor
}

type LookupResponse struct {
	ID       string `json:"id"`
	Provider string `json:"provider"`
}

// --- suspect/dead rumors payloads ---
type SuspectRumor struct {
	RumorID string `json:"rumorID"`
	Peer    string `json:"peer"`
	Fanout  uint8  `json:"fanout,omitempty"` // B: peer random per hop
	MaxFw   uint8  `json:"maxfw,omitempty"`  // F: volte che inoltro la stessa rumor
	TTL     uint8  `json:"ttl,omitempty"`    // hop residui
}

type DeadRumor struct {
	RumorID string `json:"rumorID"`
	Peer    string `json:"peer"`
	Fanout  uint8  `json:"fanout,omitempty"`
	MaxFw   uint8  `json:"maxfw,omitempty"`
	TTL     uint8  `json:"ttl,omitempty"`
}

// ---------- generic helpers ----------
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

// ---------- heartbeat-digest helpers ----------
/*
func EncodeHeartbeatDigest(from string, hbHeartbeatLight) ([]byte, error) {
	return Encode(MsgHeartbeatLight from, hb)
}*/

func DecodeHeartbeatLight(raw json.RawMessage) (HeartbeatLight, error) {
	var hb HeartbeatLight
	err := json.Unmarshal(raw, &hb)
	return hb, err
}

func DecodeHeartbeat(raw json.RawMessage) (Heartbeat, error) {
	var hb Heartbeat
	err := json.Unmarshal(raw, &hb)
	return hb, err
}

func DecodeRepairReq(raw json.RawMessage) (RepairReq, error) {
	var r RepairReq
	err := json.Unmarshal(raw, &r)
	return r, err
}

// ---------- lookup helpers ----------
func DecodeLookupRequest(raw json.RawMessage) (LookupRequest, error) {
	var lr LookupRequest
	err := json.Unmarshal(raw, &lr)
	return lr, err
}

func DecodeLookupResponse(raw json.RawMessage) (LookupResponse, error) {
	var resp LookupResponse
	err := json.Unmarshal(raw, &resp)
	return resp, err
}

// ---------- suspect/dead decoders ----------
func DecodeSuspectRumor(raw json.RawMessage) (SuspectRumor, error) {
	var r SuspectRumor
	err := json.Unmarshal(raw, &r)
	return r, err
}

func DecodeDeadRumor(raw json.RawMessage) (DeadRumor, error) {
	var d DeadRumor
	err := json.Unmarshal(raw, &d)
	return d, err
}

// decoder per Leave
func DecodeLeave(raw json.RawMessage) (Leave, error) {
	var lv Leave
	err := json.Unmarshal(raw, &lv)
	return lv, err
}
