package blocks

// BlockSize is the size of the data unit used by storm.
const BlockSize int64 = 32 * 1024 // 8 KiB

// BlockType is the enum representing the block type.
type BlockType byte

// Block types. singularity block is not here because there is only one such block, it is always kept separately
// and never cached.
const (
	FreeBlockType BlockType = iota
	PointerBlockType
	LeafBlockType
)

// Hash represents hash.
type Hash uint64

// BlockAddress is the address (index or offset) of the block.
type BlockAddress uint64

// Block defines the constraint for generics using block types.
type Block interface {
	comparable
}

// ObjectID is the ID of the object in storm.
type ObjectID uint64

// SpaceID is the ID of the space in storm.
type SpaceID uint64

// Pointer is a pointer to other block.
type Pointer struct {
	Checksum      Hash
	Address       BlockAddress
	BirthRevision uint64
}
