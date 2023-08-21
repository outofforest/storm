package v0

import (
	"crypto/sha256"

	"github.com/outofforest/photon"
	"github.com/pkg/errors"

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

// ComputeChecksums computes struct and data checksums of the block.
func (b Block) ComputeChecksums() (blocks.Hash, blocks.Hash, error) {
	hash := sha256.New()
	for i, state := range b.RecordStates {
		if state != FreeRecordState {
			_, err := hash.Write(b.Records[i].Key[:])
			if err != nil {
				return blocks.Hash{}, blocks.Hash{}, errors.WithStack(err)
			}
			_, err = hash.Write(b.Records[i].Value[:])
			if err != nil {
				return blocks.Hash{}, blocks.Hash{}, errors.WithStack(err)
			}
		}
	}
	return sha256.Sum256(photon.NewFromValue(&b).B), blocks.Hash(hash.Sum(nil)), nil
}
