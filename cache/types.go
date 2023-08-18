package cache

import (
	"github.com/outofforest/photon"

	"github.com/outofforest/storm/types"
)

const (
	// CacheHeaderSize is the maximum size of the header in cached block.
	// This number is hardcoded instead of taking `unsafe.Sizeof(Header{})` to ensure proper alignment
	// of the following block structure.
	CacheHeaderSize = 8

	// CachedBlockSize is the size of the cached block stored in memory.
	CachedBlockSize = types.BlockSize + CacheHeaderSize
)

// Header stores the metadata of cached block.
type Header struct {
	Address types.BlockAddress
}

// CachedBlock represents block stored in cache.
type CachedBlock[T types.Block] struct {
	Header *Header
	Block  photon.Union[T]
}
