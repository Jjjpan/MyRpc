package encode

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
)

type JsonCodec struct {
	conn    io.ReadWriteCloser
	buf     *bufio.Writer
	decoder *json.Decoder
	encoder *json.Encoder
}

var _ Codec = (*JsonCodec)(nil)

func NewJsonCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &JsonCodec{
		conn:    conn,
		buf:     buf,
		decoder: json.NewDecoder(conn),
		encoder: json.NewEncoder(buf),
	}
}

func (codec *JsonCodec) ReadHeader(header *Header) error {
	return codec.decoder.Decode(header)
}

func (codec *JsonCodec) ReadBody(body interface{}) error {
	return codec.decoder.Decode(body)
}

func (codec *JsonCodec) Write(header *Header, body interface{}) (err error) {
	defer func() {
		//_, err = codec.buf.WriteString("PANYIJIAN")
		fmt.Println("Write buffer length: ", codec.buf.Buffered())
		err = codec.buf.Flush()
		if err != nil {
			_ = codec.Close()
		}
	}()
	if err := codec.encoder.Encode(header); err != nil {
		log.Println("rpc stub: Json error:", err)
		return err
	}
	if err := codec.encoder.Encode(body); err != nil {
		log.Println("rpc stub: Json error:", err)
		return err
	}
	return nil
}

func (codec *JsonCodec) Close() error {
	return codec.conn.Close()
}
