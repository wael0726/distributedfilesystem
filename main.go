package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wael0726/distributedfilesystem/p2p"
)

func makeServer(listenAddr string, bootstrapNodes ...string) *FileServer {
	tcpOpts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
	}

	transport := p2p.NewTCPTransport(tcpOpts)

	opts := FileServerOpts{
		EncKey:            newEncryptionKey(),
		StorageRoot:       listenAddr + "_network",
		PathTransformFunc: CASPathTransformFunc,
		Transport:         transport,
		BootstrapNodes:    bootstrapNodes,
	}

	srv := NewFileServer(opts)
	transport.OnPeer = srv.OnPeer

	return srv
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Create and start servers
	s1 := makeServer(":3000")
	s2 := makeServer(":7000")
	s3 := makeServer(":5000", ":3000", ":7000")

	servers := []*FileServer{s1, s2, s3}

	for _, srv := range servers {
		srv := srv // capture
		go func() {
			log.Printf("Starting server on %s", srv.Transport.Addr())
			if err := srv.Start(); err != nil {
				log.Printf("Server %s failed: %v", srv.Transport.Addr(), err)
			}
		}()
	}

	time.Sleep(3 * time.Second)

	const numFiles = 10

	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("demo_file_%d.txt", i)
		content := []byte(fmt.Sprintf("This is demo content for file %d", i))

		log.Printf("Storing %s on node %s", key, s3.Transport.Addr())

		if err := s3.Store(key, bytes.NewReader(content)); err != nil {
			log.Printf("Store failed for %s: %v", key, err)
			continue
		}

		if err := s3.store.Delete(s3.ID, key); err != nil {
			log.Printf("Local delete failed for %s: %v", key, err)
		}

		r, err := s3.Get(key)
		if err != nil {
			log.Printf("Get failed for %s: %v", key, err)
			continue
		}

		data, err := io.ReadAll(r)
		if err != nil {
			log.Printf("Read failed for %s: %v", key, err)
			continue
		}

		fmt.Printf("Retrieved %s: %s\n", key, string(data))
	}

	<-ctx.Done()

	log.Println("Shutting down servers...")
	for _, srv := range servers {
		srv.Stop()
	}
	time.Sleep(500 * time.Millisecond)
	log.Println("All servers stopped.")
}