package cache

import (
	"crypto/rand"
	"io"
	"testing"

	"github.com/outofforest/photon"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/pointer"
	"github.com/outofforest/storm/blocks/singularity"
	"github.com/outofforest/storm/persistence"
	"github.com/outofforest/storm/pkg/memdev"
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

	block := cache.SingularityBlock()
	requireT.NoError(err)

	checksum := block.Checksum
	block.Checksum = 0

	requireT.Equal(blocks.BlockChecksum(block), checksum)
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

	cache.singularityBlock.V.LastAllocatedBlock++

	// Set new block directly on dev and read it from cache to test that data are correctly loaded to it.

	newBlock := photon.NewFromValue(&pointer.Block{})
	newBlock.V.Pointers[pointerIndex].Address = 21

	_, err = dev.Seek(blocks.BlockSize, io.SeekStart)
	requireT.NoError(err)
	_, err = dev.Write(newBlock.B)
	requireT.NoError(err)

	block, blockMeta, err := fetchBlock[pointer.Block](cache, &blocks.Pointer{
		Checksum: blocks.BlockChecksum(newBlock.V),
		Address:  1,
	})
	requireT.NoError(err)
	requireT.Equal(blocks.BlockAddress(1), blockMeta.Address)
	requireT.Equal(blocks.BlockAddress(21), block.Pointers[pointerIndex].Address)

	// Modify block directly on dev and read it from cache again to verify that cache returns unmodified cached version.

	newBlock.V.Pointers[pointerIndex].Address = 22

	_, err = dev.Seek(blocks.BlockSize, io.SeekStart)
	requireT.NoError(err)
	_, err = dev.Write(newBlock.B)
	requireT.NoError(err)

	block, blockMeta, err = fetchBlock[pointer.Block](cache, &blocks.Pointer{
		Checksum: blocks.BlockChecksum(newBlock.V),
		Address:  1,
	})
	requireT.NoError(err)
	requireT.Equal(blocks.BlockAddress(1), blockMeta.Address)
	requireT.Equal(blocks.BlockAddress(21), block.Pointers[pointerIndex].Address)
}

func TestNewBlock(t *testing.T) {
	requireT := require.New(t)

	const pointerIndex = 3

	dev := memdev.New(devSize)
	requireT.NoError(persistence.Initialize(dev, false))

	store, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	cache, err := New(store, cacheSize)
	requireT.NoError(err)

	// Fetching block before it is created returns an error

	_, _, err = fetchBlock[pointer.Block](cache, &blocks.Pointer{
		Address: 2,
	})
	requireT.Error(err)

	// Create new block

	newBlock, newBlockMeta, err := newBlock[pointer.Block](cache)
	requireT.NoError(err)
	newBlock.Pointers[pointerIndex].Address = 21

	// Block should be allocated

	requireT.Equal(blocks.BlockAddress(1), cache.singularityBlock.V.LastAllocatedBlock)
	requireT.Equal(blocks.BlockAddress(1), newBlockMeta.Address)

	// Fetching block from cache should return new block

	block, blockMeta, err := fetchBlock[pointer.Block](cache, &blocks.Pointer{
		Checksum:      blocks.BlockChecksum(newBlock),
		Address:       newBlockMeta.Address,
		BirthRevision: 1,
	})

	requireT.NoError(err)
	requireT.Equal(blocks.BlockAddress(21), block.Pointers[pointerIndex].Address)
	requireT.Equal(blocks.BlockAddress(1), blockMeta.Address)

	// Update block

	block.Pointers[pointerIndex].Address = 22

	// Nothing should be updated so far on dev

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	devSBlock := photon.NewFromValue(&singularity.Block{})
	_, err = dev.Read(devSBlock.B)
	requireT.NoError(err)

	requireT.Equal(blocks.BlockAddress(0), devSBlock.V.LastAllocatedBlock)

	// Committing changes

	requireT.NoError(err, cache.Commit())

	// Blocks on disk should be updated

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	devSBlock = photon.NewFromValue(&singularity.Block{})
	_, err = dev.Read(devSBlock.B)
	requireT.NoError(err)

	_, err = dev.Seek(blocks.BlockSize, io.SeekStart)
	requireT.NoError(err)

	devNewBlock := photon.NewFromValue(&pointer.Block{})
	_, err = dev.Read(devNewBlock.B)
	requireT.NoError(err)

	requireT.Equal(blocks.BlockAddress(1), devSBlock.V.LastAllocatedBlock)
	requireT.Equal(blocks.BlockAddress(22), devNewBlock.V.Pointers[pointerIndex].Address)

	block, blockMeta, err = fetchBlock[pointer.Block](cache, &blocks.Pointer{
		Checksum:      blocks.BlockChecksum(devNewBlock.V),
		Address:       1,
		BirthRevision: 1,
	})
	requireT.NoError(err)
	requireT.Equal(blocks.BlockAddress(22), block.Pointers[pointerIndex].Address)
	requireT.Equal(blocks.BlockAddress(1), blockMeta.Address)

	// Create new cache, read blocks and verify that new values are there

	cache2, err := New(store, cacheSize)
	requireT.NoError(err)

	block, blockMeta, err = fetchBlock[pointer.Block](cache2, &blocks.Pointer{
		Checksum:      blocks.BlockChecksum(devNewBlock.V),
		Address:       1,
		BirthRevision: 1,
	})
	requireT.NoError(err)

	requireT.Equal(blocks.BlockAddress(1), cache2.singularityBlock.V.LastAllocatedBlock)
	requireT.Equal(blocks.BlockAddress(22), block.Pointers[pointerIndex].Address)
	requireT.Equal(blocks.BlockAddress(1), blockMeta.Address)
}

func TestChecksumIsVerifiedWhenFetching(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(persistence.Initialize(dev, false))

	store, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	cache, err := New(store, cacheSize)
	requireT.NoError(err)

	// Create new block

	newBlock, newBlockMeta, err := newBlock[pointer.Block](cache)
	requireT.NoError(err)

	// Block is in cache so fetching with invalid checksum should work

	_, _, err = fetchBlock[pointer.Block](cache, &blocks.Pointer{
		Address:       newBlockMeta.Address,
		BirthRevision: 1,
	})
	requireT.NoError(err)

	// Fetching from cold cache with invalid checksum should fail

	requireT.NoError(cache.Commit())

	cache2, err := New(store, cacheSize)
	requireT.NoError(err)

	_, _, err = fetchBlock[pointer.Block](cache2, &blocks.Pointer{
		Address:       newBlockMeta.Address,
		BirthRevision: 1,
	})
	requireT.Error(err)

	// It should succeed once correct checksum is provided

	_, _, err = fetchBlock[pointer.Block](cache2, &blocks.Pointer{
		Checksum:      blocks.BlockChecksum(newBlock),
		Address:       newBlockMeta.Address,
		BirthRevision: 1,
	})
	requireT.NoError(err)

	// Again, once block is in cache checksum doesn't matter

	_, _, err = fetchBlock[pointer.Block](cache2, &blocks.Pointer{
		Address:       newBlockMeta.Address,
		BirthRevision: 1,
	})
	requireT.NoError(err)
}

// paddedStruct is intentionally designed in a way causing padding
type paddedStruct struct {
	Field1 uint64
	Field2 byte
	Field3 uint64
}

func TestNewBlocksProduceConsistentResult(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(persistence.Initialize(dev, false))

	store, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	cache, err := New(store, cacheSize)
	requireT.NoError(err)

	// Randomize cached data to ensure that paddings in objects are correctly zeroed

	randomizeCache(t, cache)

	// Create and commit two identical block

	newBlock1, newBlock1Meta, err := newBlock[paddedStruct](cache)
	requireT.NoError(err)
	newBlock1.Field1 = 1
	newBlock1.Field2 = 0x02
	newBlock1.Field3 = 3

	newBlock2, newBlock2Meta, err := newBlock[paddedStruct](cache)
	requireT.NoError(err)
	newBlock2.Field1 = 1
	newBlock2.Field2 = 0x02
	newBlock2.Field3 = 3

	// Content should match
	requireT.NotEqual(newBlock1Meta.Address, newBlock2Meta.Address)
	requireT.Equal(photon.NewFromValue(newBlock1).B, photon.NewFromValue(newBlock2).B)
}

func randomizeCache(t *testing.T, c *Cache) {
	_, err := rand.Read(c.data)
	require.NoError(t, err)
}
