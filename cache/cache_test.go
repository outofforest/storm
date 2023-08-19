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

	block, err := FetchBlock[types.SingularityBlock](cache, 0)
	requireT.NoError(err)

	storedChecksum := block.StructChecksum
	block.StructChecksum = types.Hash{}
	checksumComputed := types.Hash(sha256.Sum256(photon.NewFromValue(&block).B))

	requireT.Equal(checksumComputed, storedChecksum)
}

func TestReadByAddress(t *testing.T) {
	requireT := require.New(t)

	const pointerIndex = 3

	dev := memdev.New(devSize)
	requireT.NoError(persistence.Initialize(dev, false))

	store, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	cache, err := New(store, cacheSize)
	requireT.NoError(err)

	cache.SingularityBlock().LastAllocatedBlock++

	// Set new block directly on dev and read it from cache to test that data are correctly loaded to it.

	newBlock := photon.NewFromValue(&types.PointerBlock{})
	newBlock.V.Pointers[pointerIndex].Address = 21

	_, err = dev.Seek(types.BlockSize, io.SeekStart)
	requireT.NoError(err)
	_, err = dev.Write(newBlock.B)
	requireT.NoError(err)

	block, err := FetchBlock[types.PointerBlock](cache, 1)
	requireT.NoError(err)

	requireT.Equal(types.BlockAddress(21), block.Pointers[pointerIndex].Address)

	// Modify block directly on dev and read it from cache again to verify that cache returns unmodified cached version.

	newBlock.V.Pointers[pointerIndex].Address = 22

	_, err = dev.Seek(types.BlockSize, io.SeekStart)
	requireT.NoError(err)
	_, err = dev.Write(newBlock.B)
	requireT.NoError(err)

	block, err = FetchBlock[types.PointerBlock](cache, 1)
	requireT.NoError(err)

	requireT.Equal(types.BlockAddress(21), block.Pointers[pointerIndex].Address)
}

func TestWriteNewBlock(t *testing.T) {
	requireT := require.New(t)

	const pointerIndex = 3

	dev := memdev.New(devSize)
	requireT.NoError(persistence.Initialize(dev, false))

	store, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	cache, err := New(store, cacheSize)
	requireT.NoError(err)

	// Fetching block before it is created returns an error

	_, err = FetchBlock[types.PointerBlock](cache, 2)
	requireT.Error(err)

	// Create new block

	var newBlock types.PointerBlock
	newBlock.Pointers[pointerIndex].Address = 21

	address, err := NewBlock(cache, newBlock)
	requireT.NoError(err)
	requireT.Equal(types.BlockAddress(2), address)

	// New block should be allocated

	requireT.Equal(types.BlockAddress(2), cache.SingularityBlock().LastAllocatedBlock)

	// Fetching block from cache should work now

	block, err := FetchBlock[types.PointerBlock](cache, 2)
	requireT.NoError(err)
	requireT.Equal(types.BlockAddress(21), block.Pointers[pointerIndex].Address)

	// Nothing should be updated so far on dev

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	devSBlock := photon.NewFromValue(&types.SingularityBlock{})
	_, err = dev.Read(devSBlock.B)
	requireT.NoError(err)

	requireT.Equal(types.BlockAddress(1), devSBlock.V.LastAllocatedBlock)

	// Syncing changes

	requireT.NoError(err, cache.Sync())

	// Blocks on disk should be updated

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	devSBlock = photon.NewFromValue(&types.SingularityBlock{})
	_, err = dev.Read(devSBlock.B)
	requireT.NoError(err)

	_, err = dev.Seek(2*types.BlockSize, io.SeekStart)
	requireT.NoError(err)

	devNewBlock := photon.NewFromValue(&types.PointerBlock{})
	_, err = dev.Read(devNewBlock.B)
	requireT.NoError(err)

	requireT.Equal(types.BlockAddress(2), devSBlock.V.LastAllocatedBlock)
	requireT.Equal(types.BlockAddress(21), devNewBlock.V.Pointers[pointerIndex].Address)

	// Create new cache, read blocks and verify that new values are there

	cache2, err := New(store, cacheSize)
	requireT.NoError(err)

	sBlock := cache2.SingularityBlock()
	newBlock, err = FetchBlock[types.PointerBlock](cache2, 2)
	requireT.NoError(err)

	requireT.Equal(types.BlockAddress(2), sBlock.LastAllocatedBlock)
	requireT.Equal(types.BlockAddress(21), newBlock.Pointers[pointerIndex].Address)
}
