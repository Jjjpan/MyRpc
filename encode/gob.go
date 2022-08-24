package encode

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn    io.ReadWriteCloser
	buf     *bufio.Writer
	decoder *gob.Decoder
	encoder *gob.Encoder
}

var _ Codec = (*GobCodec)(nil)

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn:    conn,
		buf:     buf,
		decoder: gob.NewDecoder(conn),
		encoder: gob.NewEncoder(buf),
	}
}

func (codec *GobCodec) ReadHeader(header *Header) error {
	return codec.decoder.Decode(header)
}

func (codec *GobCodec) ReadBody(body interface{}) error {
	return codec.decoder.Decode(body)
}

func (codec *GobCodec) Write(header *Header, body interface{}) (err error) {
	defer func() {
		err = codec.buf.Flush()
		if err != nil {
			_ = codec.Close()
		}
	}()
	if err := codec.encoder.Encode(header); err != nil {
		log.Println("rpc stub: Gob error:", err)
		return err
	}
	if err := codec.encoder.Encode(body); err != nil {
		log.Println("Gob:", err)
		return err
	}
	return nil
}

func (codec *GobCodec) Close() error {
	return codec.conn.Close()
}
