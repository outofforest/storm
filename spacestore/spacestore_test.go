package spacestore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/cache"
	"github.com/outofforest/storm/persistence"
	"github.com/outofforest/storm/pkg/memdev"
)

const (
	devSize   = 1024 * 1024 * 10 // 10MiB
	cacheSize = 1024 * 1024 * 5  // 5MiB
)

func TestSetGet(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(persistence.Initialize(dev, false))

	s, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	c, err := cache.New(s, cacheSize)
	requireT.NoError(err)

	var blockType blocks.BlockType
	origin := cache.BlockOrigin{
		Pointer:   &blocks.Pointer{},
		BlockType: &blockType,
	}

	// Space does not exist

	_, _, exists, err := GetSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.False(exists)

	// Set the space

	keyOrigin, objectOrigin, trace, err := EnsureSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.Equal(blocks.FreeBlockType, *keyOrigin.BlockType)
	requireT.Equal(blocks.FreeBlockType, *objectOrigin.BlockType)

	*keyOrigin.BlockType = blocks.LeafBlockType
	*objectOrigin.BlockType = blocks.LeafBlockType

	trace.Commit()
	trace.Commit()

	// Get the space now

	keyOrigin, objectOrigin, exists, err = GetSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(blocks.LeafBlockType, *keyOrigin.BlockType)
	requireT.Equal(blocks.LeafBlockType, *objectOrigin.BlockType)

	// Ensuring space again should return the same space

	keyOrigin, objectOrigin, trace, err = EnsureSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.Equal(blocks.LeafBlockType, *keyOrigin.BlockType)
	requireT.Equal(blocks.LeafBlockType, *objectOrigin.BlockType)

	trace.Release()
	trace.Release()

	// Commit changes

	requireT.NoError(c.Commit())

	// Create new cache and verify that objects exist

	c, err = cache.New(s, cacheSize)
	requireT.NoError(err)

	keyOrigin, objectOrigin, trace, err = EnsureSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.Equal(blocks.LeafBlockType, *keyOrigin.BlockType)
	requireT.Equal(blocks.LeafBlockType, *objectOrigin.BlockType)

	trace.Release()

	keyOrigin, objectOrigin, exists, err = GetSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(blocks.LeafBlockType, *keyOrigin.BlockType)
	requireT.Equal(blocks.LeafBlockType, *objectOrigin.BlockType)
}

func TestStoringBatches(t *testing.T) {
	const (
		nBatches  = 600
		batchSize = 5
	)

	requireT := require.New(t)

	dev := memdev.New(1024 * 1024 * 1024)
	requireT.NoError(persistence.Initialize(dev, false))

	s, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	// Cache is intentionally small to ensure that it behaves correctly when cached blocks are unloaded.
	c, err := cache.New(s, 15*blocks.BlockSize)
	requireT.NoError(err)

	var blockType blocks.BlockType
	origin := cache.BlockOrigin{
		Pointer:   &blocks.Pointer{},
		BlockType: &blockType,
	}

	for k, i := 0, 0; i < nBatches; i++ {
		startK := k
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			keyOrigin, storeOrigin, trace, err := EnsureSpace(c, origin, blocks.SpaceID(k))
			requireT.NoError(err)
			*keyOrigin.Pointer = blocks.Pointer{
				Address: blocks.BlockAddress(k),
			}
			*storeOrigin.Pointer = blocks.Pointer{
				Address: blocks.BlockAddress(k),
			}

			trace.Commit()
			trace.Commit()
		}

		// Get objects before committing

		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			keyOrigin, storeOrigin, exists, err := GetSpace(c, origin, blocks.SpaceID(k))
			requireT.NoError(err)
			requireT.True(exists)
			requireT.Equal(blocks.BlockAddress(k), keyOrigin.Pointer.Address)
			requireT.Equal(blocks.BlockAddress(k), storeOrigin.Pointer.Address)
		}

		// Commit changes
		requireT.NoError(c.Commit())

		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			keyOrigin, storeOrigin, trace, err := EnsureSpace(c, origin, blocks.SpaceID(k))
			requireT.NoError(err)
			*keyOrigin.Pointer = blocks.Pointer{
				Address: blocks.BlockAddress(k),
			}
			*storeOrigin.Pointer = blocks.Pointer{
				Address: blocks.BlockAddress(k),
			}

			trace.Release()
			trace.Release()
		}
	}

	// Get objects

	for k, i := 0, 0; i < nBatches; i++ {
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			keyOrigin, storeOrigin, exists, err := GetSpace(c, origin, blocks.SpaceID(k))
			requireT.NoError(err)
			requireT.True(exists)
			requireT.Equal(blocks.BlockAddress(k), keyOrigin.Pointer.Address)
			requireT.Equal(blocks.BlockAddress(k), storeOrigin.Pointer.Address)
		}
	}

	// Create new cache

	c, err = cache.New(s, 15*blocks.BlockSize)
	requireT.NoError(err)

	// Get objects

	for k, i := 0, 0; i < nBatches; i++ {
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			keyOrigin, storeOrigin, exists, err := GetSpace(c, origin, blocks.SpaceID(k))
			requireT.NoError(err)
			requireT.True(exists)
			requireT.Equal(blocks.BlockAddress(k), keyOrigin.Pointer.Address)
			requireT.Equal(blocks.BlockAddress(k), storeOrigin.Pointer.Address)
		}
	}
}
