package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

const (
	defaultRootFolderName = "ggnetwork"
	maxFileSize           = 1 << 30
)

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blocksize := 5
	sliceLen := len(hashStr) / blocksize
	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i*blocksize, (i*blocksize)+blocksize
		paths[i] = hashStr[from:to]
	}

	return PathKey{
		PathName: strings.Join(paths, "/"),
		Filename: hashStr,
	}
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	PathName string
	Filename string
}

func (p PathKey) FirstPathName() string {
	paths := strings.Split(p.PathName, "/")
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.Filename)
}

type StoreOpts struct {
	Root              string
	PathTransformFunc PathTransformFunc
	MaxFileSize       int64 // 0 = illimité
}

var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{PathName: key, Filename: key}
}

type Store struct {
	StoreOpts
	mu sync.RWMutex
}

func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}
	if len(opts.Root) == 0 {
		opts.Root = defaultRootFolderName
	}
	if opts.MaxFileSize <= 0 {
		opts.MaxFileSize = maxFileSize
	}

	return &Store{StoreOpts: opts}
}

func (s *Store) Has(id, key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pathKey := s.PathTransformFunc(key)
	fullPath := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	_, err := os.Stat(fullPath)
	return !errors.Is(err, os.ErrNotExist)
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}

func (s *Store) Delete(id, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pathKey := s.PathTransformFunc(key)

	defer log.Printf("deleted [%s] from disk (node %s)", pathKey.Filename, id)

	firstPath := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FirstPathName())
	return os.RemoveAll(firstPath)
}

func (s *Store) Write(id, key string, r io.Reader) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeStream(id, key, r)
}

func (s *Store) WriteDecrypt(encKey []byte, id, key string, r io.Reader) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	n, err := copyDecrypt(encKey, r, f)
	if err != nil {
		_ = os.Remove(f.Name())
		return 0, err
	}

	if err := f.Sync(); err != nil {
		log.Printf("fsync failed after decrypt write: %v", err)
	}

	return int64(n), nil
}

func (s *Store) openFileForWriting(id, key string) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)
	dir := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.PathName)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir failed: %w", err)
	}

	path := fmt.Sprintf("%s/%s", dir, pathKey.Filename)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create failed: %w", err)
	}

	return f, nil
}

func (s *Store) writeStream(id, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	lr := io.LimitReader(r, s.MaxFileSize+1)
	n, err := io.Copy(f, lr)
	if err != nil {
		_ = os.Remove(f.Name())
		return 0, err
	}

	if n > s.MaxFileSize {
		_ = os.Remove(f.Name())
		return 0, errors.New("file exceeds maximum allowed size")
	}

	if err := f.Sync(); err != nil {
		log.Printf("fsync failed after write: %v", err)
	}

	return n, nil
}

func (s *Store) Read(id, key string) (int64, io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pathKey := s.PathTransformFunc(key)
	path := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	f, err := os.Open(path)
	if err != nil {
		return 0, nil, fmt.Errorf("open failed: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return 0, nil, fmt.Errorf("stat failed: %w", err)
	}

	return fi.Size(), f, nil
}