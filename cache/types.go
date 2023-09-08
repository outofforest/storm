package cache

import (
	"github.com/outofforest/storm/blocks"
)

// blockState is the enum representing the state of the block.
type blockState byte

// Enum of possible block states
const (
	freeBlockState blockState = iota
	usedBlockState
	invalidBlockState
)

type metadata struct {
	Data           []byte
	Address        blocks.BlockAddress
	BirthRevision  uint64
	NCommits       uint64
	NReferences    uint64
	State          blockState
	PostCommitFunc func() error
}

// BlockOrigin tracks information collected during traversing the tree up to the leaf block.
type BlockOrigin struct {
	Pointer   *blocks.Pointer
	BlockType *blocks.BlockType

	parentBlockMeta *metadata
}

// Block represents block stored in cache.
type Block[T blocks.Block] struct {
	meta  *metadata
	Block *T
}

// Address returns address of the block.
func (b Block[T]) Address() blocks.BlockAddress {
	return b.meta.Address
}

// BirthRevision returns revision when block was created.
func (b Block[T]) BirthRevision() uint64 {
	return b.meta.BirthRevision
}
