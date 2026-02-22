package p2p

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type TCPPeer struct {
	net.Conn
	outbound bool
	wg       *sync.WaitGroup
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		Conn:     conn,
		outbound: outbound,
		wg:       &sync.WaitGroup{},
	}
}

func (p *TCPPeer) CloseStream() {
	p.wg.Done()
}

func (p *TCPPeer) Send(b []byte) error {
	if len(b) > MaxMessageSize {
		return errors.New("payload too large")
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))

	if _, err := p.Conn.Write(lenBuf[:]); err != nil {
		return err
	}

	_, err := p.Conn.Write(b)
	return err
}

type TCPTransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakeFunc
	Decoder       Decoder
	OnPeer        func(Peer) error
}

type TCPTransport struct {
	TCPTransportOpts
	listener net.Listener
	rpcch    chan RPC
}

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport {
	if opts.Decoder == nil {
		opts.Decoder = DefaultDecoder
	}

	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcch:            make(chan RPC),
	}
}

func (t *TCPTransport) Addr() string {
	return t.ListenAddr
}

func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcch
}

func (t *TCPTransport) Close() error {
	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}

func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	go t.handleConn(conn, true)
	return nil
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error
	t.listener, err = net.Listen("tcp", t.ListenAddr)
	if err != nil {
		return err
	}

	log.Printf("TCP transport listening on %s", t.ListenAddr)
	go t.startAcceptLoop()
	return nil
}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go t.handleConn(conn, false)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	defer func() {
		log.Printf("dropping connection from %s: %v", conn.RemoteAddr(), conn.Close())
	}()

	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	_ = conn.SetWriteDeadline(time.Now().Add(60 * time.Second))

	peer := NewTCPPeer(conn, outbound)

	if err := t.HandshakeFunc(peer); err != nil {
		log.Printf("handshake failed with %s: %v", conn.RemoteAddr(), err)
		return
	}

	if t.OnPeer != nil {
		if err := t.OnPeer(peer); err != nil {
			log.Printf("OnPeer callback failed: %v", err)
			return
		}
	}

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if _, err := conn.Write([]byte{}); err != nil {
				conn.Close()
				return
			}
		}
	}()

	for {
		rpc := RPC{}
		err := t.Decoder.Decode(conn, &rpc)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("read timeout from %s", conn.RemoteAddr())
			} else if err != io.EOF {
				log.Printf("decode error from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}

		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_ = conn.SetWriteDeadline(time.Now().Add(60 * time.Second))

		rpc.From = conn.RemoteAddr().String()

		if rpc.Stream {
			peer.wg.Add(1)
			log.Printf("incoming stream from %s, waiting...", conn.RemoteAddr())
			peer.wg.Wait()
			log.Printf("stream closed from %s, resuming read loop", conn.RemoteAddr())
			continue
		}

		select {
		case t.rpcch <- rpc:
		default:
			log.Printf("rpc channel full from %s - dropping message", conn.RemoteAddr())
		}
	}
}