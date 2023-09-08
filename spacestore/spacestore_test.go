package spacestore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/spacelist"
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

	_, exists, err := GetSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.False(exists)

	// Set the space

	space, trace, err := EnsureSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.NotNil(space)
	requireT.Equal(spacelist.FreeSpaceState, space.State)

	space.State = spacelist.DefinedSpaceState
	trace.Commit()

	// Get the space now

	space, exists, err = GetSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(spacelist.DefinedSpaceState, space.State)

	// Ensuring space again should return the same space

	space, trace, err = EnsureSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.NotNil(space)
	requireT.Equal(spacelist.DefinedSpaceState, space.State)

	trace.Release()

	// Commit changes

	requireT.NoError(c.Commit())

	// Create new cache and verify that objects exist

	c, err = cache.New(s, cacheSize)
	requireT.NoError(err)

	space, trace, err = EnsureSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.NotNil(space)
	requireT.Equal(spacelist.DefinedSpaceState, space.State)

	trace.Release()

	space, exists, err = GetSpace(c, origin, 1)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(spacelist.DefinedSpaceState, space.State)
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
			space, trace, err := EnsureSpace(c, origin, blocks.SpaceID(k))
			requireT.NoError(err)
			requireT.NotNil(space)
			requireT.Equal(spacelist.FreeSpaceState, space.State)
			requireT.Equal(blocks.FreeBlockType, space.KeyStoreBlockType)
			requireT.Equal(blocks.FreeBlockType, space.ObjectStoreBlockType)

			space.State = spacelist.DefinedSpaceState
			space.KeyStoreBlockType = blocks.LeafBlockType
			space.ObjectStoreBlockType = blocks.LeafBlockType

			trace.Commit()
		}

		// Get objects before committing

		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			space, exists, err := GetSpace(c, origin, blocks.SpaceID(k))
			requireT.NoError(err)
			requireT.True(exists)
			requireT.NotNil(space)
			requireT.Equal(spacelist.DefinedSpaceState, space.State)
			requireT.Equal(blocks.LeafBlockType, space.KeyStoreBlockType)
			requireT.Equal(blocks.LeafBlockType, space.ObjectStoreBlockType)
		}

		// Commit changes
		requireT.NoError(c.Commit())

		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			space, trace, err := EnsureSpace(c, origin, blocks.SpaceID(k))
			requireT.NoError(err)
			requireT.NotNil(space)
			requireT.Equal(spacelist.DefinedSpaceState, space.State)
			requireT.Equal(blocks.LeafBlockType, space.KeyStoreBlockType)
			requireT.Equal(blocks.LeafBlockType, space.ObjectStoreBlockType)

			trace.Release()
		}
	}

	// Get objects

	for k, i := 0, 0; i < nBatches; i++ {
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			space, exists, err := GetSpace(c, origin, blocks.SpaceID(k))
			requireT.NoError(err)
			requireT.True(exists)
			requireT.NotNil(space)
			requireT.Equal(spacelist.DefinedSpaceState, space.State)
			requireT.Equal(blocks.LeafBlockType, space.KeyStoreBlockType)
			requireT.Equal(blocks.LeafBlockType, space.ObjectStoreBlockType)
		}
	}

	// Create new cache

	c, err = cache.New(s, 15*blocks.BlockSize)
	requireT.NoError(err)

	// Get objects

	for k, i := 0, 0; i < nBatches; i++ {
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			space, exists, err := GetSpace(c, origin, blocks.SpaceID(k))
			requireT.NoError(err)
			requireT.True(exists)
			requireT.NotNil(space)
			requireT.Equal(spacelist.DefinedSpaceState, space.State)
			requireT.Equal(blocks.LeafBlockType, space.KeyStoreBlockType)
			requireT.Equal(blocks.LeafBlockType, space.ObjectStoreBlockType)
		}
	}
}
