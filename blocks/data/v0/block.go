package v0

import (
	"github.com/outofforest/photon"

	"github.com/outofforest/storm/blocks"
)

// TODO (wojciech): Currently data blocks store only a fixed set of key-value pairs with their types being strict.

// RecordsPerBlock is the number of records in each data block.
const RecordsPerBlock = 16

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

// Block contains key-value pairs.
type Block struct {
	NUsedRecords uint64
	Records      [RecordsPerBlock]Record
	RecordHashes [RecordsPerBlock]uint64
	RecordStates [RecordsPerBlock]RecordState
}

// ComputeChecksum computes checksum of the block.
func (b Block) ComputeChecksum() blocks.Hash {
	return blocks.Checksum(photon.NewFromValue(&b).B)
}
