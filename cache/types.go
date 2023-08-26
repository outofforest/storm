package cache

import (
	"unsafe"

	"github.com/outofforest/storm/blocks"
)

const (
	// CachedBlockSize is the size of the cached block stored in memory.
	CachedBlockSize = int64(unsafe.Sizeof(block[dummyBlock]{}))

	// CacheHeaderSize is the size of the header in cached block.
	CacheHeaderSize = CachedBlockSize - blocks.BlockSize
)

// blockState is the enum representing the state of the block.
type blockState byte

// Enum of possible block states
const (
	freeBlockState blockState = iota
	fetchedBlockState
	newBlockState
	invalidBlockState
)

// header stores the metadata of cached block.
type header struct {
	Address blocks.BlockAddress
	State   blockState
}

// block represents block in cache.
type block[T blocks.Block] struct {
	Header header
	Block  T
}

// dummyBlock is a dummy structure used to measure the size of block in cache.
type dummyBlock struct {
	Content [blocks.BlockSize]byte
}

func (b dummyBlock) ComputeChecksum() blocks.Hash {
	return 0
}
