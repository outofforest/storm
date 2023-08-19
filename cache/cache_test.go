package cache

import (
	"crypto/sha256"
	"io"
	"testing"

	"github.com/outofforest/photon"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/persistence"
	"github.com/outofforest/storm/pkg/memdev"
	"github.com/outofforest/storm/types"
)

const (
	devSize   = 1024 * 1024 * 10 // 10MiB
	cacheSize = 1024 * 1024 * 5  // 5MiB
)

func TestReadSingularityBlock(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(persistence.Initialize(dev, false))

	store, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	cache, err := New(store, cacheSize)
	requireT.NoError(err)

	block, err := GetBlock[types.SingularityBlock](cache, 0)
	requireT.NoError(err)

	requireT.Equal(types.BlockAddress(0), block.Header.Address)

	storedChecksum := block.Block.V.Checksum
	block.Block.V.Checksum = types.Hash{}
	checksumComputed := types.Hash(sha256.Sum256(block.Block.B))

	requireT.Equal(checksumComputed, storedChecksum)
}

func TestReadByAddress(t *testing.T) {
	requireT := require.New(t)

	const pointerIndex = 3

	dev := memdev.New(devSize)
	requireT.NoError(persistence.Initialize(dev, false))

	newBlock := photon.NewFromValue(&types.PointerBlock{})
	newBlock.V.Pointers[pointerIndex].Address = 21

	_, err := dev.Seek(types.BlockSize, io.SeekStart)
	requireT.NoError(err)
	_, err = dev.Write(newBlock.B)
	requireT.NoError(err)

	store, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	cache, err := New(store, cacheSize)
	requireT.NoError(err)

	block, err := GetBlock[types.PointerBlock](cache, 1)
	requireT.NoError(err)

	requireT.Equal(types.BlockAddress(1), block.Header.Address)
	requireT.Equal(types.BlockAddress(21), block.Block.V.Pointers[pointerIndex].Address)
}
