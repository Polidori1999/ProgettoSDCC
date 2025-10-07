package proto

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

// invio un frame JSON su TCP con prefisso di 4 byte (big-endian)
func WriteJSONFrame(c net.Conn, v any) error {
	// serializzo in JSON
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	// guardrail dimensione (16 << 20 = 16 MiB)
	if len(data) > 16<<20 {
		return fmt.Errorf("payload troppo grande")
	}

	// header a 4 byte con la lunghezza
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))

	// scrivo header poi payload (due write)
	if _, err := c.Write(hdr[:]); err != nil {
		return err
	}
	_, err = c.Write(data)
	return err
}

// leggo prima i 4 byte di header, poi esattamente 'n' byte di payload,
func ReadJSONFrame(c net.Conn, v any) error {
	var hdr [4]byte
	// leggo header (bloccante finché non ho tutti e 4 i byte)
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	// sanity check sulla lunghezza
	if n == 0 || n > 16<<20 {
		return fmt.Errorf("lunghezza frame non valida: %d", n)
	}
	// leggo esattamente n byte di payload
	buf := make([]byte, n)
	if _, err := io.ReadFull(c, buf); err != nil {
		return err
	}
	// decodifico il JSON nel value target
	return json.Unmarshal(buf, v)
}
