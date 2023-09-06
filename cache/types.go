package cache

import (
	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/pointer"
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
	NReferences    uint64
	State          blockState
	PostCommitFunc func() error
}

// BlockOrigin tracks information collected during traversing the tree up to the leaf block.
type BlockOrigin struct {
	PointerBlock Block[pointer.Block]
	Pointer      *pointer.Pointer
	BlockType    *blocks.BlockType
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

// WithPostCommitFunc sets post commit function executed after storing block to the device.
func (b Block[T]) WithPostCommitFunc(postCommitFunc func() error) {
	b.meta.PostCommitFunc = postCommitFunc
}

// IncrementReferences increments the number of references to this block.
func (b Block[T]) IncrementReferences() {
	b.meta.NReferences++
}

// DecrementReferences decrements the number of references to this block.
func (b Block[T]) DecrementReferences() {
	b.meta.NReferences--
}

// IsValid returns true if instance of the object is valid, meaning that it contains valid data and is not just a default instance.
func (b Block[T]) IsValid() bool {
	return b.meta != nil
}
