package cache

import (
	"unsafe"

	"github.com/outofforest/storm/blocks"
)

const (
	// alignment specifies the alignment requirements of the architecture
	alignment = 8

	// CacheHeaderSize is the maximum size of the header in cached block.
	// This magic ensures that the header size is a multiplication of 8, meaning that block data following the header are
	// correctly aligned.
	CacheHeaderSize = (int64(unsafe.Sizeof(header{})-1)/alignment + 1) * alignment

	// CachedBlockSize is the size of the cached block stored in memory.
	CachedBlockSize = blocks.BlockSize + CacheHeaderSize
)

// blockState is the enum representing the state of the block.
type blockState byte

// Enum of possible block states
const (
	freeBlockState blockState = iota
	fetchedBlockState
	newBlockState
)

// header stores the metadata of cached block.
type header struct {
	Address blocks.BlockAddress
	State   blockState
}
