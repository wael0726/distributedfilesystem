package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCASPathTransformFunc(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      string
		wantPath string
		wantFile string
	}{
		{
			name:     "catpicture",
			key:      "catpicture.jpg",
			wantPath: "f1d3f/628e2/3b4c5/9a8d7/e6f5g/4h3i2/j1k0l/m9n8o",
			wantFile: "f1d3f628e23b4c59a8d7e6f5g4h3i2j1k0lm9n8o",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := CASPathTransformFunc(tt.key)
			if got.PathName != tt.wantPath {
				t.Errorf("PathName mismatch\n got:  %q\n want: %q", got.PathName, tt.wantPath)
			}
			if got.Filename != tt.wantFile {
				t.Errorf("Filename mismatch\n got:  %q\n want: %q", got.Filename, tt.wantFile)
			}
		})
	}
}

func TestStoreOperations(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	nodeID := generateID()

	t.Cleanup(func() {
		if err := store.Clear(); err != nil {
			t.Errorf("cleanup failed: %v", err)
		}
	})

	testCases := []struct {
		name string
		key  string
		data []byte
	}{
		{"small image", "photo_001.jpg", []byte("fake jpeg content")},
		{"pdf document", "report.pdf", bytes.Repeat([]byte("page\n"), 100)},
		{"empty file", "empty.txt", []byte{}},
		{"very long key", strings.Repeat("x", 200), []byte("important data")},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Write
			written, err := store.Write(nodeID, tc.key, bytes.NewReader(tc.data))
			if err != nil {
				t.Fatalf("Write failed: %v", err)
			}
			if written != int64(len(tc.data)) {
				t.Errorf("written bytes = %d, want %d", written, len(tc.data))
			}

			// Exists check
			if !store.Has(nodeID, tc.key) {
				t.Fatal("file should exist after successful write")
			}

			// Read back
			size, r, err := store.Read(nodeID, tc.key)
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}
			if size != int64(len(tc.data)) {
				t.Errorf("read size = %d, want %d", size, len(tc.data))
			}

			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("io.ReadAll failed: %v", err)
			}
			if !bytes.Equal(got, tc.data) {
				t.Errorf("content mismatch for key %q\n got (%d bytes): %q\nwant (%d bytes): %q",
					tc.key, len(got), got[:min(100, len(got))], len(tc.data), tc.data[:min(100, len(tc.data))])
			}

			// Delete
			if err := store.Delete(nodeID, tc.key); err != nil {
				t.Fatalf("Delete failed: %v", err)
			}

			if store.Has(nodeID, tc.key) {
				t.Error("file still exists after Delete")
			}
		})
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	tmpDir := t.TempDir()
	opts := StoreOpts{
		Root:              filepath.Join(tmpDir, "ggnetwork"),
		PathTransformFunc: CASPathTransformFunc,
	}

	s := NewStore(opts)

	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})

	return s
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}