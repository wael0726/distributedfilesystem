package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

func generateID() string {
	b := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// hashKey computes SHA-256 of the key and returns it as hex.
func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// newEncryptionKey generates a random 256-bit AES key.
func newEncryptionKey() []byte {
	k := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		panic(err)
	}
	return k
}

func copyStream(stream cipher.Stream, src io.Reader, dst io.Writer) (int64, error) {
	var total int64
	buf := make([]byte, 32*1024)

	for {
		n, err := src.Read(buf)
		if n > 0 {
			stream.XORKeyStream(buf, buf[:n])
			nn, werr := dst.Write(buf[:n])
			total += int64(nn)
			if werr != nil {
				return total, werr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func copyEncrypt(key []byte, src io.Reader, dst io.Writer) (int64, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return 0, fmt.Errorf("cipher creation: %w", err)
	}

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return 0, fmt.Errorf("IV generation: %w", err)
	}

	if _, err := dst.Write(iv); err != nil {
		return 0, fmt.Errorf("IV write: %w", err)
	}

	stream := cipher.NewCTR(block, iv)
	n, err := copyStream(stream, src, dst)
	return n + int64(len(iv)), err
}

func copyDecrypt(key []byte, src io.Reader, dst io.Writer) (int64, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return 0, fmt.Errorf("cipher creation: %w", err)
	}

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(src, iv); err != nil {
		return 0, fmt.Errorf("IV read: %w", err)
	}

	stream := cipher.NewCTR(block, iv)
	return copyStream(stream, src, dst)
}