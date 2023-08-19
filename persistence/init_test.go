package persistence

import (
	"crypto/sha256"
	"io"
	"testing"

	"github.com/outofforest/photon"
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

	requireT.EqualValues(stormSubject, sBlock.V.StormID&stormSubject)
	requireT.EqualValues(dev.Size()/types.BlockSize, int64(sBlock.V.NBlocks))
	requireT.Less(dev.Size(), int64(sBlock.V.NBlocks+1)*types.BlockSize)
	requireT.EqualValues(0, sBlock.V.LastAllocatedBlock)

	checksum := sBlock.V.Checksum
	sBlock.V.Checksum = types.Hash{}
	checksumExpected := types.Hash(sha256.Sum256(sBlock.B))

	requireT.Equal(checksumExpected, checksum)
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
	requireT.NotEqual(previousSBlock.V.Checksum, newSBlock.V.Checksum)
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
