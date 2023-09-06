package v0

import (
	"github.com/outofforest/storm/blocks"
)

// TODO (wojciech): Hash data separately

// Pointer is a pointer to other block.
type Pointer struct {
	Checksum      blocks.Hash
	Address       blocks.BlockAddress
	BirthRevision uint64
}

// Block is the block forming tree. It contains pointers to other blocks.
type Block struct {
	Pointers             [PointersPerBlock]Pointer
	PointedBlockVersions [PointersPerBlock]blocks.SchemaVersion
	PointedBlockTypes    [PointersPerBlock]blocks.BlockType
}
