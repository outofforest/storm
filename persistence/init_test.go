package persistence

import (
	"io"
	"testing"

	"github.com/outofforest/photon"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/singularity"
	"github.com/outofforest/storm/pkg/memdev"
)

const devSize = 1024 * 1024 * 10 // 10MiB

func TestInit(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(Initialize(dev, false))

	_, err := dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	sBlock := photon.NewFromValue(&singularity.Block{})
	_, err = dev.Read(sBlock.B)
	requireT.NoError(err)

	checksum := sBlock.V.Checksum
	sBlock.V.Checksum = 0

	requireT.EqualValues(stormSubject, sBlock.V.StormID&stormSubject)
	requireT.EqualValues(0, sBlock.V.Revision)
	requireT.EqualValues(dev.Size()/blocks.BlockSize, int64(sBlock.V.NBlocks))
	requireT.Less(dev.Size(), int64(sBlock.V.NBlocks+1)*blocks.BlockSize)
	requireT.EqualValues(0, sBlock.V.LastAllocatedBlock)
	requireT.EqualValues(0, sBlock.V.SpacePointer.Address)
	requireT.EqualValues(blocks.FreeBlockType, sBlock.V.SpaceBlockType)
	requireT.Equal(blocks.BlockChecksum(sBlock.V), checksum)
}

func TestOverwrite(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(Initialize(dev, false))

	_, err := dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	previousSBlock := photon.NewFromValue(&singularity.Block{})
	_, err = dev.Read(previousSBlock.B)
	requireT.NoError(err)

	requireT.ErrorIs(Initialize(dev, false), ErrAlreadyInitialized)

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	sameSBlock := photon.NewFromValue(&singularity.Block{})
	_, err = dev.Read(sameSBlock.B)
	requireT.NoError(err)
	requireT.Equal(previousSBlock.V, sameSBlock.V)

	requireT.NoError(Initialize(dev, true))

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	newSBlock := photon.NewFromValue(&singularity.Block{})
	_, err = dev.Read(newSBlock.B)
	requireT.NoError(err)
	requireT.NotEqual(previousSBlock.V.Checksum, newSBlock.V.Checksum)
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
