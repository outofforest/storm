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

func TestFetchSingularityBlock(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(persistence.Initialize(dev, false))

	store, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	cache, err := New(store, cacheSize)
	requireT.NoError(err)

	block, err := FetchBlock[types.SingularityBlock](cache, 0)
	requireT.NoError(err)

	storedChecksum := block.Block.StructChecksum
	block.Block.StructChecksum = types.Hash{}
	checksumComputed := types.Hash(sha256.Sum256(photon.NewFromValue(&block.Block).B))

	requireT.Equal(checksumComputed, storedChecksum)
}

func TestFetchBlockByAddress(t *testing.T) {
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

	requireT.Equal(types.BlockAddress(21), block.Block.Pointers[pointerIndex].Address)

	// Modify block directly on dev and read it from cache again to verify that cache returns unmodified cached version.

	newBlock.V.Pointers[pointerIndex].Address = 22

	_, err = dev.Seek(types.BlockSize, io.SeekStart)
	requireT.NoError(err)
	_, err = dev.Write(newBlock.B)
	requireT.NoError(err)

	block, err = FetchBlock[types.PointerBlock](cache, 1)
	requireT.NoError(err)

	requireT.Equal(types.BlockAddress(21), block.Block.Pointers[pointerIndex].Address)
}

func TestCommitNewBlock(t *testing.T) {
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

	newBlock := NewBlock[types.PointerBlock](cache)
	newBlock.Block.Pointers[pointerIndex].Address = 21

	// Block is not allocated yet.

	requireT.Equal(types.BlockAddress(1), cache.SingularityBlock().LastAllocatedBlock)
	requireT.Equal(types.BlockAddress(0), newBlock.header.Address)
	requireT.Equal(freeBlockState, newBlock.header.State)

	// Block should be allocated after committing

	newBlock, err = newBlock.Commit()
	requireT.NoError(err)
	requireT.Equal(types.BlockAddress(2), cache.SingularityBlock().LastAllocatedBlock)
	requireT.Equal(types.BlockAddress(2), newBlock.header.Address)
	requireT.Equal(newBlockState, newBlock.header.State)

	// Fetching block from cache should work now

	block, err := FetchBlock[types.PointerBlock](cache, 2)
	requireT.NoError(err)
	requireT.Equal(types.BlockAddress(21), block.Block.Pointers[pointerIndex].Address)
	requireT.Equal(types.BlockAddress(2), block.header.Address)
	requireT.Equal(newBlockState, block.header.State)

	// Update block

	block.Block.Pointers[pointerIndex].Address = 22
	block, err = block.Commit()
	requireT.NoError(err)

	// No new block should be allocated

	requireT.Equal(types.BlockAddress(2), cache.SingularityBlock().LastAllocatedBlock)
	requireT.Equal(types.BlockAddress(2), block.header.Address)
	requireT.Equal(newBlockState, block.header.State)

	// Nothing should be updated so far on dev

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	devSBlock := photon.NewFromValue(&types.SingularityBlock{})
	_, err = dev.Read(devSBlock.B)
	requireT.NoError(err)

	requireT.Equal(types.BlockAddress(1), devSBlock.V.LastAllocatedBlock)

	// Committing changes

	requireT.NoError(err, cache.Commit())

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
	requireT.Equal(types.BlockAddress(22), devNewBlock.V.Pointers[pointerIndex].Address)

	// Status of the block should be `fetched`

	block, err = FetchBlock[types.PointerBlock](cache, 2)
	requireT.NoError(err)
	requireT.Equal(types.BlockAddress(22), block.Block.Pointers[pointerIndex].Address)
	requireT.Equal(types.BlockAddress(2), block.header.Address)
	requireT.Equal(fetchedBlockState, block.header.State)

	// Create new cache, read blocks and verify that new values are there

	cache2, err := New(store, cacheSize)
	requireT.NoError(err)

	sBlock := cache2.SingularityBlock()
	block, err = FetchBlock[types.PointerBlock](cache2, 2)
	requireT.NoError(err)

	requireT.Equal(types.BlockAddress(2), sBlock.LastAllocatedBlock)
	requireT.Equal(types.BlockAddress(22), block.Block.Pointers[pointerIndex].Address)
	requireT.Equal(fetchedBlockState, block.header.State)
	requireT.Equal(types.BlockAddress(2), block.header.Address)

	// Updating fetched block should create a new one

	block.Block.Pointers[pointerIndex].Address = 23
	nextBlock, err := block.Commit()
	requireT.NoError(err)
	requireT.Equal(types.BlockAddress(3), cache2.SingularityBlock().LastAllocatedBlock)
	requireT.Equal(types.BlockAddress(3), nextBlock.header.Address)
	requireT.Equal(newBlockState, nextBlock.header.State)
}
