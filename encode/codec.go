package encode

import (
	"io"
)

type Header struct {
	ServiceMethod string
	Seq           uint64
	Err           string
}

type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCodecFunc func(io.ReadWriteCloser) Codec

const (
	GobType  string = "application/gob"
	JsonType string = "application/json"
)

var NewCodecFuncMap map[string]NewCodecFunc

// 支持多种编解码
// TODO Protobuf编解码
func init() {
	NewCodecFuncMap = make(map[string]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
	NewCodecFuncMap[JsonType] = NewJsonCodec
}
