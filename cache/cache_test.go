package cache

import (
	"crypto/rand"
	"io"
	"testing"

	"github.com/outofforest/photon"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/blocks"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
	singularityV0 "github.com/outofforest/storm/blocks/singularity/v0"
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

	requireT.Equal(block.ComputeChecksum(), block.Checksum)
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

	newBlock := photon.NewFromValue(&pointerV0.Block{})
	newBlock.V.Pointers[pointerIndex].Address = 21

	_, err = dev.Seek(blocks.BlockSize, io.SeekStart)
	requireT.NoError(err)
	_, err = dev.Write(newBlock.B)
	requireT.NoError(err)

	block, err := FetchBlock[pointerV0.Block](cache, pointerV0.Pointer{
		Checksum: newBlock.V.ComputeChecksum(),
		Address:  1,
	})
	requireT.NoError(err)

	requireT.Equal(blocks.BlockAddress(21), block.Block.Pointers[pointerIndex].Address)

	// Modify block directly on dev and read it from cache again to verify that cache returns unmodified cached version.

	newBlock.V.Pointers[pointerIndex].Address = 22

	_, err = dev.Seek(blocks.BlockSize, io.SeekStart)
	requireT.NoError(err)
	_, err = dev.Write(newBlock.B)
	requireT.NoError(err)

	block, err = FetchBlock[pointerV0.Block](cache, pointerV0.Pointer{
		Checksum: newBlock.V.ComputeChecksum(),
		Address:  1,
	})
	requireT.NoError(err)

	requireT.Equal(blocks.BlockAddress(21), block.Block.Pointers[pointerIndex].Address)
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

	_, err = FetchBlock[pointerV0.Block](cache, pointerV0.Pointer{
		Address: 2,
	})
	requireT.Error(err)

	// Create new block

	newBlock := NewBlock[pointerV0.Block](cache)
	newBlock.Block.Pointers[pointerIndex].Address = 21

	// Block is not allocated yet.

	requireT.Equal(blocks.BlockAddress(0), cache.singularityBlock.V.LastAllocatedBlock)
	requireT.Nil(newBlock.block.V)

	// Address method returns error

	address, err := newBlock.Address()
	requireT.Error(err)
	requireT.Equal(blocks.BlockAddress(0), address)

	// Block should be allocated after committing

	newBlock, err = newBlock.Commit()
	requireT.NoError(err)
	requireT.Equal(blocks.BlockAddress(1), cache.singularityBlock.V.LastAllocatedBlock)
	requireT.Equal(blocks.BlockAddress(1), newBlock.block.V.Header.Address)
	requireT.Equal(newBlockState, newBlock.block.V.Header.State)

	// Address method should work now

	address, err = newBlock.Address()
	requireT.NoError(err)
	requireT.Equal(blocks.BlockAddress(1), address)

	// Fetching block from cache should work now

	block, err := FetchBlock[pointerV0.Block](cache, pointerV0.Pointer{
		Checksum: newBlock.Block.ComputeChecksum(),
		Address:  address,
	})

	requireT.NoError(err)
	requireT.Equal(blocks.BlockAddress(21), block.Block.Pointers[pointerIndex].Address)
	requireT.Equal(blocks.BlockAddress(1), block.block.V.Header.Address)
	requireT.Equal(newBlockState, block.block.V.Header.State)

	// Update block

	block.Block.Pointers[pointerIndex].Address = 22
	block, err = block.Commit()
	requireT.NoError(err)

	// No new block should be allocated

	requireT.Equal(blocks.BlockAddress(1), cache.singularityBlock.V.LastAllocatedBlock)
	requireT.Equal(blocks.BlockAddress(1), block.block.V.Header.Address)
	requireT.Equal(newBlockState, block.block.V.Header.State)

	// Nothing should be updated so far on dev

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	devSBlock := photon.NewFromValue(&singularityV0.Block{})
	_, err = dev.Read(devSBlock.B)
	requireT.NoError(err)

	requireT.Equal(blocks.BlockAddress(0), devSBlock.V.LastAllocatedBlock)

	// Committing changes

	requireT.NoError(err, cache.Commit())

	// Blocks on disk should be updated

	_, err = dev.Seek(0, io.SeekStart)
	requireT.NoError(err)

	devSBlock = photon.NewFromValue(&singularityV0.Block{})
	_, err = dev.Read(devSBlock.B)
	requireT.NoError(err)

	_, err = dev.Seek(blocks.BlockSize, io.SeekStart)
	requireT.NoError(err)

	devNewBlock := photon.NewFromValue(&pointerV0.Block{})
	_, err = dev.Read(devNewBlock.B)
	requireT.NoError(err)

	requireT.Equal(blocks.BlockAddress(1), devSBlock.V.LastAllocatedBlock)
	requireT.Equal(blocks.BlockAddress(22), devNewBlock.V.Pointers[pointerIndex].Address)

	// Status of the block should be `fetched`

	block, err = FetchBlock[pointerV0.Block](cache, pointerV0.Pointer{
		Checksum: devNewBlock.V.ComputeChecksum(),
		Address:  1,
	})
	requireT.NoError(err)
	requireT.Equal(blocks.BlockAddress(22), block.Block.Pointers[pointerIndex].Address)
	requireT.Equal(blocks.BlockAddress(1), block.block.V.Header.Address)
	requireT.Equal(fetchedBlockState, block.block.V.Header.State)

	// Create new cache, read blocks and verify that new values are there

	cache2, err := New(store, cacheSize)
	requireT.NoError(err)

	block, err = FetchBlock[pointerV0.Block](cache2, pointerV0.Pointer{
		Checksum: devNewBlock.V.ComputeChecksum(),
		Address:  1,
	})
	requireT.NoError(err)

	requireT.Equal(blocks.BlockAddress(1), cache2.singularityBlock.V.LastAllocatedBlock)
	requireT.Equal(blocks.BlockAddress(22), block.Block.Pointers[pointerIndex].Address)
	requireT.Equal(fetchedBlockState, block.block.V.Header.State)
	requireT.Equal(blocks.BlockAddress(1), block.block.V.Header.Address)

	// Updating fetched block should create a new one

	block.Block.Pointers[pointerIndex].Address = 23
	nextBlock, err := block.Commit()
	requireT.NoError(err)
	requireT.Equal(blocks.BlockAddress(2), cache2.singularityBlock.V.LastAllocatedBlock)
	requireT.Equal(blocks.BlockAddress(2), nextBlock.block.V.Header.Address)
	requireT.Equal(newBlockState, nextBlock.block.V.Header.State)
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

	newBlock := NewBlock[pointerV0.Block](cache)
	newBlock, err = newBlock.Commit()
	requireT.NoError(err)
	address, err := newBlock.Address()
	requireT.NoError(err)

	// Block is in cache so fetching with invalid checksum should work

	_, err = FetchBlock[pointerV0.Block](cache, pointerV0.Pointer{
		Address: address,
	})
	requireT.NoError(err)

	// Fetching from cold fresh with invalid checksum should fail

	requireT.NoError(cache.Commit())
	cache2, err := New(store, cacheSize)
	requireT.NoError(err)

	_, err = FetchBlock[pointerV0.Block](cache2, pointerV0.Pointer{
		Address: address,
	})
	requireT.Error(err)

	// It should succeed once correct checksum is provided

	_, err = FetchBlock[pointerV0.Block](cache2, pointerV0.Pointer{
		Checksum: newBlock.Block.ComputeChecksum(),
		Address:  address,
	})
	requireT.NoError(err)

	// Again, once block is in cache checksum doesn't matter

	_, err = FetchBlock[pointerV0.Block](cache2, pointerV0.Pointer{
		Address: address,
	})
	requireT.NoError(err)
}

// paddedStruct is intentionally designed in a way causing padding
type paddedStruct struct {
	Field1 uint64
	Field2 byte
	Field3 uint64
}

func (p paddedStruct) ComputeChecksum() blocks.Hash {
	return blocks.Checksum(photon.NewFromValue(&p).B)
}

func TestNewBlocksProduceConsistentHash(t *testing.T) {
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

	newBlock1 := NewBlock[paddedStruct](cache)
	newBlock1.Block.Field1 = 1
	newBlock1.Block.Field2 = 0x02
	newBlock1.Block.Field3 = 3
	newBlock1, err = newBlock1.Commit()
	requireT.NoError(err)

	newBlock2 := NewBlock[paddedStruct](cache)
	newBlock2.Block.Field1 = 1
	newBlock2.Block.Field2 = 0x02
	newBlock2.Block.Field3 = 3
	newBlock2, err = newBlock2.Commit()
	requireT.NoError(err)

	// Content should match

	requireT.Equal(newBlock1.block.B[CacheHeaderSize:], newBlock2.block.B[CacheHeaderSize:])
}

func randomizeCache(t *testing.T, c *Cache) {
	_, err := rand.Read(c.data)
	require.NoError(t, err)
	for offset := int64(0); offset < cacheSize; offset += CachedBlockSize {
		header := photon.NewFromBytes[header](c.data[offset:])
		header.V.State = freeBlockState
	}
}
