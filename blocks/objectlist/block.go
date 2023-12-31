package objectlist

import (
	"github.com/outofforest/storm/blocks"
)

const (
	// ChunkSize is the size of the data chunk addressed by single slot.
	ChunkSize = 32

	// MaxKeyComponentLength is the maximum length of one key component.
	MaxKeyComponentLength = 256

	// SplitTrigger value must be configured in a way that at least one key might be inserted after splitting.
	SplitTrigger = ChunksPerBlock * 3 / 4
)

// ChunkState defines the state of the chunk.
type ChunkState byte

// Chunk states.
const (
	FreeChunkState ChunkState = iota
	DefinedChunkState
	InvalidChunkState
)

// Block contains links to objects.
type Block struct {
	Blob [ChunksPerBlock * ChunkSize]byte

	KeyTagReminders    [ChunksPerBlock]uint64
	ObjectLinks        [ChunksPerBlock]blocks.ObjectID
	ChunkPointers      [ChunksPerBlock]uint16
	NextChunkPointers  [ChunksPerBlock]uint16
	ChunkPointerStates [ChunksPerBlock]ChunkState

	NUsedChunks    uint16
	FreeChunkIndex uint16
}
