package cache

import "github.com/outofforest/storm/blocks"

// blockState is the enum representing the state of the block.
type blockState byte

// Enum of possible block states
const (
	freeBlockState blockState = iota
	fetchedBlockState
	newBlockState
	invalidBlockState
)

type block struct {
	Data    []byte
	Address blocks.BlockAddress
	State   blockState
}
