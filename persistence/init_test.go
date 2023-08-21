package persistence

import (
	"io"
	"testing"

	"github.com/outofforest/photon"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/blocks"
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
	requireT.Equal(sBlock.V.ComputeChecksum(), sBlock.V.Checksum)
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
