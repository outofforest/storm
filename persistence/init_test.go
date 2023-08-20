package persistence

import (
	"bytes"
	"crypto/sha256"
	"io"
	"testing"

	"github.com/outofforest/photon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/pkg/memdev"
	"github.com/outofforest/storm/types"
)

const devSize = 1024 * 1024 * 10 // 10MiB

func TestInit(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(Initialize(dev, false))

	_, err := dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	sBlock := photon.NewFromValue(&types.SingularityBlock{})
	_, err = dev.Read(sBlock.B)
	requireT.NoError(err)

	_, err = dev.Seek(types.BlockSize, io.SeekStart)
	requireT.NoError(err)

	rootDataBlock := photon.NewFromValue(&types.PointerBlock{})
	_, err = dev.Read(rootDataBlock.B)
	requireT.NoError(err)

	requireT.EqualValues(stormSubject, sBlock.V.StormID&stormSubject)
	requireT.EqualValues(0, sBlock.V.Revision)
	requireT.EqualValues(dev.Size()/types.BlockSize, int64(sBlock.V.NBlocks))
	requireT.Less(dev.Size(), int64(sBlock.V.NBlocks+1)*types.BlockSize)
	requireT.EqualValues(1, sBlock.V.LastAllocatedBlock)
	requireT.EqualValues(1, sBlock.V.RootData.Address)
	requireT.EqualValues(sha256.Sum256(rootDataBlock.B), sBlock.V.RootData.StructChecksum)
	requireT.EqualValues(pointerBlockDataChecksum(rootDataBlock.V), sBlock.V.RootData.DataChecksum)

	checksum := sBlock.V.StructChecksum
	sBlock.V.StructChecksum = types.Hash{}
	checksumExpected := types.Hash(sha256.Sum256(sBlock.B))

	requireT.Equal(checksumExpected, checksum)

	requireT.EqualValues(0, rootDataBlock.V.NUsedPointers)
	requireT.Equal([64]types.BlockType{}, rootDataBlock.V.PointedBlockTypes)
}

func TestOverwrite(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(Initialize(dev, false))

	_, err := dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	previousSBlock := photon.NewFromValue(&types.SingularityBlock{})
	_, err = dev.Read(previousSBlock.B)
	requireT.NoError(err)

	requireT.ErrorIs(Initialize(dev, false), ErrAlreadyInitialized)

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	sameSBlock := photon.NewFromValue(&types.SingularityBlock{})
	_, err = dev.Read(sameSBlock.B)
	requireT.NoError(err)
	requireT.Equal(previousSBlock.V, sameSBlock.V)

	requireT.NoError(Initialize(dev, true))

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	newSBlock := photon.NewFromValue(&types.SingularityBlock{})
	_, err = dev.Read(newSBlock.B)
	requireT.NoError(err)
	requireT.NotEqual(previousSBlock.V.StormID, newSBlock.V.StormID)
	requireT.NotEqual(previousSBlock.V.StructChecksum, newSBlock.V.StructChecksum)
	requireT.Equal(previousSBlock.V.NBlocks, newSBlock.V.NBlocks)
	requireT.Equal(previousSBlock.V.LastAllocatedBlock, newSBlock.V.LastAllocatedBlock)
}

func TestTooSmall(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(minNBlocks * types.BlockSize)
	requireT.NoError(Initialize(dev, true))

	dev = memdev.New(minNBlocks*types.BlockSize - 1)
	requireT.Error(Initialize(dev, true))
}

func TestPointerBlockDataChecksum(t *testing.T) {
	assertT := assert.New(t)

	emptyDataHash := types.Hash{0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55}
	finalDataHash := types.Hash{0xf8, 0x18, 0xaf, 0xd3, 0x7a, 0x6d, 0xc3, 0xbc, 0x92, 0xfb, 0x44, 0x73, 0x10, 0x11, 0x27, 0x70, 0x6, 0xdb, 0x4e, 0xfa, 0x6e, 0x90, 0x23, 0xcd, 0x74, 0x68, 0xc0, 0x23, 0x35, 0xd2, 0x2a, 0x4d}

	pBlock := &types.PointerBlock{}
	assertT.Equal(emptyDataHash, pointerBlockDataChecksum(pBlock))

	// Only allocated pointers are taken into account

	pBlock.Pointers[0].DataChecksum = types.Hash(bytes.Repeat([]byte{0x01}, types.HashSize))
	pBlock.Pointers[1].DataChecksum = types.Hash(bytes.Repeat([]byte{0x02}, types.HashSize))
	assertT.Equal(emptyDataHash, pointerBlockDataChecksum(pBlock))

	pBlock.PointedBlockTypes[0] = types.DataBlockType
	assertT.Equal(types.Hash{0x72, 0xcd, 0x6e, 0x84, 0x22, 0xc4, 0x7, 0xfb, 0x6d, 0x9, 0x86, 0x90, 0xf1, 0x13, 0xb, 0x7d, 0xed, 0x7e, 0xc2, 0xf7, 0xf5, 0xe1, 0xd3, 0xb, 0xd9, 0xd5, 0x21, 0xf0, 0x15, 0x36, 0x37, 0x93}, pointerBlockDataChecksum(pBlock))

	pBlock.PointedBlockTypes[1] = types.DataBlockType
	assertT.Equal(finalDataHash, pointerBlockDataChecksum(pBlock))

	// Other fields don't matter

	pBlock.NUsedPointers = 2
	pBlock.Pointers[0].Address = 1
	pBlock.Pointers[0].StructChecksum = types.Hash(bytes.Repeat([]byte{0x03}, types.HashSize))
	pBlock.Pointers[1].Address = 2
	pBlock.Pointers[2].StructChecksum = types.Hash(bytes.Repeat([]byte{0x04}, types.HashSize))
	assertT.Equal(finalDataHash, pointerBlockDataChecksum(pBlock))
}
