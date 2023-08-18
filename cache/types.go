package cache

import (
	"unsafe"

	"github.com/outofforest/photon"

	"github.com/outofforest/storm/types"
)

const (
	// alignment specifies the alignment requirements of the architecture
	alignment = 8

	// CacheHeaderSize is the maximum size of the header in cached block.
	// This magic ensures that the header size is a multiplication of 8, meaning that block data following the header are
	// correctly aligned.
	CacheHeaderSize = ((unsafe.Sizeof(Header{})-1)/alignment + 1) * alignment

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
