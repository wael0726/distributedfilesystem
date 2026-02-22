package p2p

import "net"

// Peer represents a remote node in the network.
type Peer interface {
	net.Conn
	Send([]byte) error
	CloseStream()
}

type Transport interface {
	Addr() string
	Dial(addr string) error
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
}