package proto

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

func WriteJSONFrame(c net.Conn, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(data) > 16<<20 {
		return fmt.Errorf("payload troppo grande")
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := c.Write(hdr[:]); err != nil {
		return err
	}
	_, err = c.Write(data)
	return err
}

func ReadJSONFrame(c net.Conn, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > 16<<20 {
		return fmt.Errorf("lunghezza frame non valida: %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(c, buf); err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}
