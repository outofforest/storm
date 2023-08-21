package v0

import (
	"crypto/sha256"

	"github.com/outofforest/photon"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/blocks"
)

// PointersPerBlock is the number of pointers in each pointer block.
const PointersPerBlock = 64

// Pointer is a pointer to other block.
type Pointer struct {
	StructChecksum blocks.Hash
	DataChecksum   blocks.Hash
	Address        blocks.BlockAddress
}

// Block is the block forming tree. It contains pointers to other blocks.
type Block struct {
	NUsedPointers        uint64
	Pointers             [PointersPerBlock]Pointer
	PointedBlockVersions [PointersPerBlock]blocks.SchemaVersion
	PointedBlockTypes    [PointersPerBlock]blocks.BlockType
}

// ComputeChecksums computes struct and data checksums of the block.
func (b Block) ComputeChecksums() (blocks.Hash, blocks.Hash, error) {
	hash := sha256.New()
	for i, state := range b.PointedBlockTypes {
		if state != blocks.FreeBlockType {
			_, err := hash.Write(b.Pointers[i].DataChecksum[:])
			if err != nil {
				return blocks.Hash{}, blocks.Hash{}, errors.WithStack(err)
			}
		}
	}
	return sha256.Sum256(photon.NewFromValue(&b).B), blocks.Hash(hash.Sum(nil)), nil
}
