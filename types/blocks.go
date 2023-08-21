package types

import (
	"crypto/sha256"

	"github.com/outofforest/photon"
	"github.com/pkg/errors"
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
	LeafBlockType
)

// Block defines the constraint for generics using block types.
type Block interface {
	SingularityBlock | PointerBlock | DataBlock
	ComputeChecksums() (Hash, Hash, error)
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

	RootData          Pointer
	RootDataBlockType BlockType
}

// ComputeChecksums computes struct checksum of the block.
func (sb SingularityBlock) ComputeChecksums() (Hash, Hash, error) {
	sb.StructChecksum = Hash{}
	return sha256.Sum256(photon.NewFromValue(&sb).B), Hash{}, nil
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

// ComputeChecksums computes struct and data checksums of the block.
func (pb PointerBlock) ComputeChecksums() (Hash, Hash, error) {
	hash := sha256.New()
	for i, state := range pb.PointedBlockTypes {
		if state != FreeBlockType {
			_, err := hash.Write(pb.Pointers[i].DataChecksum[:])
			if err != nil {
				return Hash{}, Hash{}, errors.WithStack(err)
			}
		}
	}
	return sha256.Sum256(photon.NewFromValue(&pb).B), Hash(hash.Sum(nil)), nil
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
	Value [32]byte
}

// DataBlock contains key-value pairs.
type DataBlock struct {
	NUsedRecords uint64
	Records      [RecordsPerBlock]Record
	RecordHashes [RecordsPerBlock]uint64
	RecordStates [RecordsPerBlock]RecordState
}

// ComputeChecksums computes struct and data checksums of the block.
func (db DataBlock) ComputeChecksums() (Hash, Hash, error) {
	hash := sha256.New()
	for i, state := range db.RecordStates {
		if state != FreeRecordState {
			_, err := hash.Write(db.Records[i].Key[:])
			if err != nil {
				return Hash{}, Hash{}, errors.WithStack(err)
			}
			_, err = hash.Write(db.Records[i].Value[:])
			if err != nil {
				return Hash{}, Hash{}, errors.WithStack(err)
			}
		}
	}
	return sha256.Sum256(photon.NewFromValue(&db).B), Hash(hash.Sum(nil)), nil
}

// Hash represents hash.
type Hash [HashSize]byte

// BlockAddress is the address (index or offset) of the block.
type BlockAddress uint64
