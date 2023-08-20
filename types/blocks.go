package types

import (
	"crypto/sha256"
)

const (
	// BlockSize is the size of the data unit used by storm.
	BlockSize int64 = 128 * 1024 // 128 KiB

	// HashSize is the size of the hash used in tree.
	HashSize = sha256.Size

	// PointersPerBlock is the number of pointers in each pointer block.
	PointersPerBlock = 64

	// PointersPerBlockShift is set to number of bits used by PointersPerBlock.
	PointersPerBlockShift = 6

	// RecordsPerBlock is the number of records in each data block.
	RecordsPerBlock = 16
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
	StructChecksum Hash
	StormID        uint64
	Revision       uint64
	NBlocks        uint64

	// TODO (wojciech): Replace with correct (de)allocation mechanism
	LastAllocatedBlock BlockAddress
	RootData           Pointer
}

// Pointer is a pointer to other block.
type Pointer struct {
	StructChecksum Hash
	DataChecksum   Hash
	Address        BlockAddress
}

// PointerBlock is the block forming tree. It contains pointers to other blocks.
type PointerBlock struct {
	NUsedPointers     uint64
	Pointers          [PointersPerBlock]Pointer
	PointedBlockTypes [PointersPerBlock]BlockType
}

// TODO (wojciech): Currently data blocks store only a fixed set of key-value pairs with their types being strict.

// RecordState defines the state of the data record.
type RecordState byte

// Record states.
const (
	FreeRecordState RecordState = iota
	DefinedRecordState
)

// Record stores the key-value pair
type Record struct {
	Key   [32]byte
	Value uint64
}

// DataBlock contains key-value pairs.
type DataBlock struct {
	NUsedRecords uint64
	Records      [RecordsPerBlock]Record
	RecordHashes [RecordsPerBlock]uint64
	RecordStates [RecordsPerBlock]RecordState
}

// Hash represents hash.
type Hash [HashSize]byte

// BlockAddress is the address (index or offset) of the block.
type BlockAddress uint64
