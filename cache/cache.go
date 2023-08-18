package cache

import (
	"io"

	"github.com/outofforest/photon"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/types"
)

// Cache caches blocks.
type Cache struct {
	source io.ReadWriteSeeker
	data   []byte
}

// New creates new cache.
func New(source io.ReadWriteSeeker, size uint32) *Cache {
	return &Cache{
		source: source,
		data:   make([]byte, size),
	}
}

// Get retrieves block data from cache, if needed block is loaded from persistent source.
func (c *Cache) Get(address types.BlockAddress) (*Header, []byte, error) {
	if _, err := c.source.Seek(int64(address*types.BlockSize), io.SeekStart); err != nil {
		return nil, nil, err
	}
	n, err := c.source.Read(c.data[CacheHeaderSize:CachedBlockSize])
	if err != nil {
		return nil, nil, err
	}
	if n != types.BlockSize {
		return nil, nil, errors.Errorf("expected to read %d bytes, but got %d", types.BlockSize, n)
	}
	header := photon.NewFromBytes[Header](c.data[:CacheHeaderSize])
	header.V.Address = address

	return header.V, c.data[CacheHeaderSize:CachedBlockSize], nil
}

// GetBlock returns structure representing the block of the particular type.
func GetBlock[T types.Block](cache *Cache, address types.BlockAddress) (CachedBlock[T], error) {
	header, content, err := cache.Get(address)
	if err != nil {
		return CachedBlock[T]{}, err
	}
	return CachedBlock[T]{
		Header: header,
		Block:  photon.NewFromBytes[T](content),
	}, nil
}
