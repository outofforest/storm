package blocks

import (
	"crypto/sha256"
)

const (
	// BlockSize is the size of the data unit used by storm.
	BlockSize int64 = 128 * 1024 // 128 KiB

	// HashSize is the size of the hash used in tree.
	HashSize = sha256.Size
)

// BlockType is the enum representing the block type.
type BlockType byte

// Block types. singularity block is not here because there is only one such block, it is always kept separately
// and never cached.
const (
	FreeBlockType BlockType = iota
	PointerBlockType
	LeafBlockType
)

// SchemaVersion defines version of the schema.
type SchemaVersion uint16

// Schema versions
const (
	SingularityV0 SchemaVersion = iota
	PointerV0
	ObjectListV0
	BlobV0
)

// Hash represents hash.
type Hash [HashSize]byte

// BlockAddress is the address (index or offset) of the block.
type BlockAddress uint64

// Block defines the constraint for generics using block types.
type Block interface {
	comparable
	ComputeChecksum() Hash
}

// ObjectID is the ID of the object in storm.
type ObjectID uint64
