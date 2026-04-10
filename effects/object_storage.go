/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 */

package effects

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// ObjectStorage abstracts uploading/downloading blobs to/from object storage.
type ObjectStorage interface {
	Upload(ctx context.Context, path string, data []byte) error
	Download(ctx context.Context, path string) ([]byte, error)
}

// BunnyStorage implements ObjectStorage using Bunny CDN's storage API.
type BunnyStorage struct {
	uploadEndpoint string // e.g. "https://storage.bunnycdn.com/zone"
	cdnEndpoint    string // e.g. "https://cdn.example.com"
	accessKey      string
	client         *http.Client
}

// NewBunnyStorage creates a BunnyStorage instance.
func NewBunnyStorage(uploadEndpoint, cdnEndpoint, accessKey string) *BunnyStorage {
	return &BunnyStorage{
		uploadEndpoint: uploadEndpoint,
		cdnEndpoint:    cdnEndpoint,
		accessKey:      accessKey,
		client:         &http.Client{},
	}
}

func (b *BunnyStorage) Upload(ctx context.Context, path string, data []byte) error {
	url := b.uploadEndpoint + "/" + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, io.NopCloser(
		io.NewSectionReader(newBytesReaderAt(data), 0, int64(len(data))),
	))
	if err != nil {
		return fmt.Errorf("bunny upload: %w", err)
	}
	req.Header.Set("AccessKey", b.accessKey)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(data))

	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("bunny upload: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bunny upload: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (b *BunnyStorage) Download(ctx context.Context, path string) ([]byte, error) {
	url := b.cdnEndpoint + "/" + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("bunny download: %w", err)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bunny download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bunny download: status %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

// bytesReaderAt wraps []byte for use with io.NewSectionReader.
type bytesReaderAt struct {
	data []byte
}

func newBytesReaderAt(data []byte) *bytesReaderAt {
	return &bytesReaderAt{data: data}
}

func (r *bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// MemoryStorage is an in-memory ObjectStorage implementation for testing.
type MemoryStorage struct {
	mu    sync.RWMutex
	blobs map[string][]byte
}

// NewMemoryStorage creates a new MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{blobs: make(map[string][]byte)}
}

func (m *MemoryStorage) Upload(_ context.Context, path string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.blobs[path] = cp
	return nil
}

func (m *MemoryStorage) Download(_ context.Context, path string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.blobs[path]
	if !ok {
		return nil, fmt.Errorf("not found: %s", path)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

// Keys returns all stored paths (for testing).
func (m *MemoryStorage) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.blobs))
	for k := range m.blobs {
		keys = append(keys, k)
	}
	return keys
}
