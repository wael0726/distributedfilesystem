package main

import (
	"bytes"
	"testing"
)

func TestEncryptDecryptRoundtrip(t *testing.T) {
	t.Parallel()

	key := newEncryptionKey()

	cases := []struct {
		name    string
		payload []byte
	}{
		{"empty", []byte{}},
		{"short text", []byte("secret message")},
		{"long text", bytes.Repeat([]byte("hello crypto "), 1000)},
		{"binary", []byte{0x00, 0xFF, 0xAA, 0x55, 0x01, 0x02, 0x03}},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			var enc bytes.Buffer
			encN, err := copyEncrypt(key, bytes.NewReader(c.payload), &enc)
			if err != nil {
				t.Fatalf("encrypt failed: %v", err)
			}
			if encN != int64(len(c.payload)+16) {
				t.Errorf("encrypted bytes = %d, want %d", encN, len(c.payload)+16)
			}

			var dec bytes.Buffer
			decN, err := copyDecrypt(key, bytes.NewReader(enc.Bytes()), &dec)
			if err != nil {
				t.Fatalf("decrypt failed: %v", err)
			}
			if decN != int64(len(c.payload)) {
				t.Errorf("decrypted bytes = %d, want %d", decN, len(c.payload))
			}

			if !bytes.Equal(dec.Bytes(), c.payload) {
				t.Errorf("round-trip mismatch\n got: %x\nwant: %x", dec.Bytes()[:32], c.payload[:min(len(c.payload), 32)])
			}
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}