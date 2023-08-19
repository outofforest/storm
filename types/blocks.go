package types

import (
	"crypto/sha256"
)

const (
	// BlockSize is the size of the data unit used by storm.
	BlockSize = 128 * 1024 // 128 KiB

	// HashSize is the size of the hash used in tree.
	HashSize = sha256.Size

	// PointersPerBlock is the number of pointers in each pointer block.
	PointersPerBlock = 64
)

// BlockType is the enum representing the block type.
type BlockType byte

// Block types. singularity block is not here because there is only one such block, it is always kept separately
// and never cached.
const (
	FreeBlockType BlockType = iota
	PointerBlockType
	DataBlockType
)

// Block defines the constraint for generics using block types.
type Block interface {
	SingularityBlock | PointerBlock | DataBlock
}

// BlockBytes represents the raw data bytes of the block.
type BlockBytes [BlockSize]byte

// SingularityBlock is the starting block of the store. Everything starts and ends here.
type SingularityBlock struct {
	Checksum Hash
	StormID  uint64
	NBlocks  uint64

	Data Pointer
}

// PointerBlock is the block forming tree. It contains pointers to other blocks.
type PointerBlock struct {
	Pointers [PointersPerBlock]Pointer
}

// DataBlock contains key-value pairs.
type DataBlock struct {
}

// Hash represents hash.
type Hash [HashSize]byte

// BlockAddress is the address (index or offset) of the block.
type BlockAddress uint64

// Pointer is a pointer to other block.
type Pointer struct {
	DataChecksum   Hash
	StructChecksum Hash
	Address        BlockAddress
	Type           BlockType
}
