package p2p

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

const (
	IncomingMessage = 0x1
	IncomingStream  = 0x2

	MaxMessageSize = 4 << 20 
)

type Decoder interface {
	Decode(r io.Reader, msg *RPC) error
}

type LengthPrefixedDecoder struct{}

func (d LengthPrefixedDecoder) Decode(r io.Reader, msg *RPC) error {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return err
	}

	length := binary.BigEndian.Uint32(lenBuf[:])
	if length > MaxMessageSize {
		return errors.New("message exceeds maximum size")
	}
	if length == 0 {
		return errors.New("empty message")
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}

	msg.Payload = payload
	msg.Stream = false
	return nil
}

type StreamOrMessageDecoder struct {
	MsgDecoder Decoder
}

func (d StreamOrMessageDecoder) Decode(r io.Reader, msg *RPC) error {
	peekBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, peekBuf); err != nil {
		return err
	}

	if peekBuf[0] == IncomingStream {
		msg.Stream = true
		msg.Payload = nil
		return nil
	}
	return d.MsgDecoder.Decode(io.MultiReader(bytes.NewReader(peekBuf), r), msg)
}

var DefaultDecoder Decoder = StreamOrMessageDecoder{
	MsgDecoder: LengthPrefixedDecoder{},
}