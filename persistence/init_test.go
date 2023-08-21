package persistence

import (
	"bytes"
	"io"
	"testing"

	"github.com/outofforest/photon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/blocks"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
	singularityV0 "github.com/outofforest/storm/blocks/singularity/v0"
	"github.com/outofforest/storm/pkg/memdev"
)

const devSize = 1024 * 1024 * 10 // 10MiB

func TestInit(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(Initialize(dev, false))

	_, err := dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	sBlock := photon.NewFromValue(&singularityV0.Block{})
	_, err = dev.Read(sBlock.B)
	requireT.NoError(err)

	requireT.EqualValues(blocks.SingularityV0, sBlock.V.SchemaVersion)
	requireT.EqualValues(stormSubject, sBlock.V.StormID&stormSubject)
	requireT.EqualValues(0, sBlock.V.Revision)
	requireT.EqualValues(dev.Size()/blocks.BlockSize, int64(sBlock.V.NBlocks))
	requireT.Less(dev.Size(), int64(sBlock.V.NBlocks+1)*blocks.BlockSize)
	requireT.EqualValues(1, sBlock.V.LastAllocatedBlock)
	requireT.EqualValues(0, sBlock.V.RootData.Address)
	requireT.EqualValues(blocks.FreeBlockType, sBlock.V.RootDataBlockType)

	checksum, _, err := sBlock.V.ComputeChecksums()
	requireT.NoError(err)

	requireT.Equal(checksum, sBlock.V.StructChecksum)
}

func TestOverwrite(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(Initialize(dev, false))

	_, err := dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	previousSBlock := photon.NewFromValue(&singularityV0.Block{})
	_, err = dev.Read(previousSBlock.B)
	requireT.NoError(err)

	requireT.ErrorIs(Initialize(dev, false), ErrAlreadyInitialized)

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	sameSBlock := photon.NewFromValue(&singularityV0.Block{})
	_, err = dev.Read(sameSBlock.B)
	requireT.NoError(err)
	requireT.Equal(previousSBlock.V, sameSBlock.V)

	requireT.NoError(Initialize(dev, true))

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	newSBlock := photon.NewFromValue(&singularityV0.Block{})
	_, err = dev.Read(newSBlock.B)
	requireT.NoError(err)
	requireT.Equal(previousSBlock.V.SchemaVersion, newSBlock.V.SchemaVersion)
	requireT.NotEqual(previousSBlock.V.StructChecksum, newSBlock.V.StructChecksum)
	requireT.NotEqual(previousSBlock.V.StormID, newSBlock.V.StormID)
	requireT.Equal(previousSBlock.V.NBlocks, newSBlock.V.NBlocks)
	requireT.Equal(previousSBlock.V.LastAllocatedBlock, newSBlock.V.LastAllocatedBlock)
}

func TestTooSmall(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(minNBlocks * blocks.BlockSize)
	requireT.NoError(Initialize(dev, true))

	dev = memdev.New(minNBlocks*blocks.BlockSize - 1)
	requireT.Error(Initialize(dev, true))
}

func TestPointerBlockDataChecksum(t *testing.T) {
	assertT := assert.New(t)

	emptyDataHash := blocks.Hash{0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55}
	finalDataHash := blocks.Hash{0xf8, 0x18, 0xaf, 0xd3, 0x7a, 0x6d, 0xc3, 0xbc, 0x92, 0xfb, 0x44, 0x73, 0x10, 0x11, 0x27, 0x70, 0x6, 0xdb, 0x4e, 0xfa, 0x6e, 0x90, 0x23, 0xcd, 0x74, 0x68, 0xc0, 0x23, 0x35, 0xd2, 0x2a, 0x4d}

	pBlock := &pointerV0.Block{}
	_, dataChecksum, err := pBlock.ComputeChecksums()
	assertT.NoError(err)
	assertT.Equal(emptyDataHash, dataChecksum)

	// Only allocated pointers are taken into account

	pBlock.Pointers[0].DataChecksum = blocks.Hash(bytes.Repeat([]byte{0x01}, blocks.HashSize))
	pBlock.Pointers[1].DataChecksum = blocks.Hash(bytes.Repeat([]byte{0x02}, blocks.HashSize))
	_, dataChecksum, err = pBlock.ComputeChecksums()
	assertT.NoError(err)
	assertT.Equal(emptyDataHash, dataChecksum)

	pBlock.PointedBlockTypes[0] = blocks.LeafBlockType
	_, dataChecksum, err = pBlock.ComputeChecksums()
	assertT.NoError(err)
	assertT.Equal(blocks.Hash{0x72, 0xcd, 0x6e, 0x84, 0x22, 0xc4, 0x7, 0xfb, 0x6d, 0x9, 0x86, 0x90, 0xf1, 0x13, 0xb, 0x7d, 0xed, 0x7e, 0xc2, 0xf7, 0xf5, 0xe1, 0xd3, 0xb, 0xd9, 0xd5, 0x21, 0xf0, 0x15, 0x36, 0x37, 0x93},
		dataChecksum)

	pBlock.PointedBlockTypes[1] = blocks.LeafBlockType
	_, dataChecksum, err = pBlock.ComputeChecksums()
	assertT.NoError(err)
	assertT.Equal(finalDataHash, dataChecksum)

	// Other fields don't matter

	pBlock.NUsedPointers = 2
	pBlock.Pointers[0].Address = 1
	pBlock.Pointers[0].StructChecksum = blocks.Hash(bytes.Repeat([]byte{0x03}, blocks.HashSize))
	pBlock.Pointers[1].Address = 2
	pBlock.Pointers[2].StructChecksum = blocks.Hash(bytes.Repeat([]byte{0x04}, blocks.HashSize))
	_, dataChecksum, err = pBlock.ComputeChecksums()
	assertT.NoError(err)
	assertT.Equal(finalDataHash, dataChecksum)
}
