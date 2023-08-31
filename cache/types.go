package cache

import "github.com/outofforest/storm/blocks"

// blockState is the enum representing the state of the block.
type blockState byte

// Enum of possible block states
const (
	freeBlockState blockState = iota
	usedBlockState
	invalidBlockState
)

type block struct {
	Data          []byte
	Address       blocks.BlockAddress
	BirthRevision uint64
	State         blockState
}
