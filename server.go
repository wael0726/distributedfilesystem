package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/wael0726/distributedfilesystem/p2p"
)

type FileServerOpts struct {
	ID                string
	EncKey            []byte
	StorageRoot       string
	PathTransformFunc PathTransformFunc
	Transport         p2p.Transport
	BootstrapNodes    []string
}

type FileServer struct {
	FileServerOpts

	peerMu sync.RWMutex
	peers  map[string]p2p.Peer

	store  *Store
	quitch chan struct{}
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}

	if len(opts.ID) == 0 {
		opts.ID = generateID()
	}

	fs := &FileServer{
		FileServerOpts: opts,
		store:          NewStore(storeOpts),
		quitch:         make(chan struct{}),
		peers:          make(map[string]p2p.Peer),
	}

	go fs.heartbeatLoop()

	return fs
}

type MessagePing struct {
	Timestamp int64
}

type MessagePong struct {
	Timestamp int64
}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	ID   string
	Key  string
	Size int64
	Hash string
}

type MessageGetFile struct {
	ID  string
	Key string
}

type MessageAck struct {
	ID      string
	Key     string
	Success bool
	Error   string
}


func (s *FileServer) heartbeatLoop() {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.peerMu.RLock()
			peers := make([]p2p.Peer, 0, len(s.peers))
			for _, p := range s.peers {
				peers = append(peers, p)
			}
			s.peerMu.RUnlock()

			for _, peer := range peers {
				go s.sendPing(peer)
			}

		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) sendPing(peer p2p.Peer) {
	ping := Message{
		Payload: MessagePing{Timestamp: time.Now().UnixMilli()},
	}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(ping); err != nil {
		return
	}

	addr := peer.RemoteAddr().String()

	if err := peer.Send([]byte{p2p.IncomingMessage}); err != nil {
		s.removeDeadPeer(addr)
		return
	}
	if err := peer.Send(buf.Bytes()); err != nil {
		s.removeDeadPeer(addr)
		return
	}
}

func (s *FileServer) removeDeadPeer(addr string) {
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	if p, ok := s.peers[addr]; ok {
		_ = p.Close()
		delete(s.peers, addr)
		log.Printf("[%s] peer retiré (mort) : %s", s.Transport.Addr(), addr)
	}
}

// Broadcast
func (s *FileServer) broadcast(msg *Message) (<-chan MessageAck, error) {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return nil, err
	}

	ackCh := make(chan MessageAck, 16)

	s.peerMu.RLock()
	peersCopy := make([]p2p.Peer, 0, len(s.peers))
	for _, p := range s.peers {
		peersCopy = append(peersCopy, p)
	}
	s.peerMu.RUnlock()

	for _, peer := range peersCopy {
		go func(p p2p.Peer) {
			_ = p.Send([]byte{p2p.IncomingMessage})
			_ = p.Send(buf.Bytes())
		}(peer)
	}

	return ackCh, nil
}

// Store
func (s *FileServer) Store(key string, r io.Reader) error {
	var teeBuf bytes.Buffer
	tee := io.TeeReader(r, &teeBuf)

	size, err := s.store.Write(s.ID, key, tee)
	if err != nil {
		return err
	}

	hash := sha256.Sum256(teeBuf.Bytes())
	hashStr := hex.EncodeToString(hash[:])

	msg := Message{
		Payload: MessageStoreFile{
			ID:   s.ID,
			Key:  hashKey(key),
			Size: size + 16,
			Hash: hashStr,
		},
	}

	ackCh, err := s.broadcast(&msg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	required := max(1, (len(s.peers)/2)+1)
	success := 0

	for success < required {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout quorum (%d/%d)", success, required)
		case ack, ok := <-ackCh:
			if !ok {
				return errors.New("ack channel closed")
			}
			if ack.Success {
				success++
			}
		}
	}

	go s.replicateToRandomPeers(key, &teeBuf, size, hashStr)

	return nil
}

func (s *FileServer) replicateToRandomPeers(key string, data *bytes.Buffer, size int64, hash string) {
	s.peerMu.RLock()
	peers := make([]p2p.Peer, 0, len(s.peers))
	for _, p := range s.peers {
		peers = append(peers, p)
	}
	s.peerMu.RUnlock()

	if len(peers) <= 1 {
		return
	}

	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })

	for i := 0; i < min(3, len(peers)); i++ {
		go func(p p2p.Peer) {
			msg := Message{Payload: MessageStoreFile{
				ID:   s.ID,
				Key:  hashKey(key),
				Size: size + 16,
				Hash: hash,
			}}
			buf := new(bytes.Buffer)
			_ = gob.NewEncoder(buf).Encode(msg)

			_ = p.Send([]byte{p2p.IncomingMessage})
			_ = p.Send(buf.Bytes())
			_ = p.Send([]byte{p2p.IncomingStream})

			_, _ = copyEncrypt(s.EncKey, bytes.NewReader(data.Bytes()), p)
		}(peers[i])
	}
}

func (s *FileServer) Get(key string) (io.Reader, error) {
	if s.store.Has(s.ID, key) {
		_, r, err := s.store.Read(s.ID, key)
		if err == nil {
			return r, nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resultCh := make(chan io.Reader, 1)
	errCh := make(chan error, 8)

	s.peerMu.RLock()
	peers := make([]p2p.Peer, 0, len(s.peers))
	for _, p := range s.peers {
		peers = append(peers, p)
	}
	s.peerMu.RUnlock()

	if len(peers) == 0 {
		return nil, errors.New("no peers available")
	}

	for _, peer := range peers {
		go func(p p2p.Peer) {
			msg := Message{Payload: MessageGetFile{ID: s.ID, Key: hashKey(key)}}
			buf := new(bytes.Buffer)
			_ = gob.NewEncoder(buf).Encode(msg)

			if err := p.Send([]byte{p2p.IncomingMessage}); err != nil {
				errCh <- err
				return
			}
			if err := p.Send(buf.Bytes()); err != nil {
				errCh <- err
				return
			}

			var fileSize int64
			if err := binary.Read(p, binary.LittleEndian, &fileSize); err != nil {
				errCh <- err
				return
			}

			n, err := s.store.WriteDecrypt(s.EncKey, s.ID, key, io.LimitReader(p, fileSize))
			if err != nil {
				errCh <- err
				return
			}

			p.CloseStream()

			_, r, err := s.store.Read(s.ID, key)
			if err != nil {
				errCh <- err
				return
			}

			select {
			case resultCh <- r:
			case <-ctx.Done():
			}
		}(peer)
	}

	select {
	case r := <-resultCh:
		return r, nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Handlers
func (s *FileServer) handleMessage(from string, msg *Message) error {
	s.peerMu.RLock()
	peer, ok := s.peers[from]
	s.peerMu.RUnlock()
	if !ok {
		return fmt.Errorf("peer %s not found", from)
	}

	switch v := msg.Payload.(type) {
	case MessagePing:
		pong := Message{Payload: MessagePong{Timestamp: v.Timestamp}}
		buf := new(bytes.Buffer)
		_ = gob.NewEncoder(buf).Encode(pong)
		peer.Send([]byte{p2p.IncomingMessage})
		_ = peer.Send(buf.Bytes())
		return nil

	case MessagePong:
		return nil

	case MessageStoreFile:
		err := s.handleMessageStoreFile(from, v)
		ack := MessageAck{ID: v.ID, Key: v.Key, Success: err == nil}
		if err != nil {
			ack.Error = err.Error()
		}
		ackMsg := Message{Payload: ack}
		buf := new(bytes.Buffer)
		if _ = gob.NewEncoder(buf).Encode(ackMsg); err == nil {
			peer.Send([]byte{p2p.IncomingMessage})
			_ = peer.Send(buf.Bytes())
		}
		return err

	case MessageGetFile:
		return s.handleMessageGetFile(from, v)

	default:
		return fmt.Errorf("unknown message type %T", v)
	}
}

func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	peer, _ := s.peers[from] 

	n, err := s.store.Write(msg.ID, msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return err
	}

	log.Printf("[%s] stored %d bytes for %s from %s", s.Transport.Addr(), n, msg.Key, from)
	peer.CloseStream()
	return nil
}

func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	if !s.store.Has(msg.ID, msg.Key) {
		return fmt.Errorf("file not found: %s", msg.Key)
	}

	size, r, err := s.store.Read(msg.ID, msg.Key)
	if err != nil {
		return err
	}
	defer func() { _ = r.(io.Closer).Close() }()

	peer, _ := s.peers[from]

	peer.Send([]byte{p2p.IncomingStream})
	binary.Write(peer, binary.LittleEndian, size)

	n, err := io.Copy(peer, r)
	log.Printf("sent %d bytes of %s to %s", n, msg.Key, from)
	return err
}

// Lifecycle
func (s *FileServer) OnPeer(p p2p.Peer) error {
	s.peerMu.Lock()
	s.peers[p.RemoteAddr().String()] = p
	s.peerMu.Unlock()
	log.Printf("peer connected: %s", p.RemoteAddr())
	return nil
}

func (s *FileServer) loop() {
	defer s.Transport.Close()

	for {
		select {
		case rpc := <-s.Transport.Consume():
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println("decode error:", err)
				continue
			}
			_ = s.handleMessage(rpc.From, &msg)

		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) bootstrapNetwork() {
	for _, addr := range s.BootstrapNodes {
		if addr == "" {
			continue
		}
		go func(a string) {
			_ = s.Transport.Dial(a)
		}(addr)
	}
}

func (s *FileServer) Start() error {
	log.Printf("[%s] starting", s.Transport.Addr())

	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	s.bootstrapNetwork()
	s.loop()
	return nil
}

func (s *FileServer) Stop() {
	close(s.quitch)
}

func init() {
	gob.Register(Message{})
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
	gob.Register(MessageAck{})
	gob.Register(MessagePing{})
	gob.Register(MessagePong{})
}

func max(a, b int) int { if a > b { return a }; return b }
func min(a, b int) int { if a < b { return a }; return b }