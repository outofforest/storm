package objectstore

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

type item struct {
	Field1 int
	Field2 int
}

func TestSetGet(t *testing.T) {
	requireT := require.New(t)

	item1 := item{
		Field1: 1,
		Field2: 1,
	}
	item2 := item{
		Field1: 2,
		Field2: 2,
	}
	item3 := item{
		Field1: 3,
		Field2: 3,
	}

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

	// Object does not exist

	_, exists, err := GetObject[item](c, origin, 10, 1)
	requireT.NoError(err)
	requireT.False(exists)

	// Set the object

	requireT.NoError(SetObject[item](c, origin, nil, 10, 1, item1))

	// Get the object now

	object1, exists, err := GetObject[item](c, origin, 10, 1)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(item1, object1)

	// Set the second object

	requireT.NoError(SetObject[item](c, origin, nil, 10, 2, item2))

	// Get objects

	object1, exists, err = GetObject[item](c, origin, 10, 1)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(item1, object1)

	object2, exists, err := GetObject[item](c, origin, 10, 2)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(item2, object2)

	// Commit changes

	requireT.NoError(c.Commit())

	// Create new cache and verify that objects exist

	c, err = cache.New(s, cacheSize)
	requireT.NoError(err)

	object1, exists, err = GetObject[item](c, origin, 10, 1)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(item1, object1)

	object2, exists, err = GetObject[item](c, origin, 10, 2)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(item2, object2)

	// Update existing object

	requireT.NoError(SetObject[item](c, origin, nil, 10, 2, item3))
	object2, exists, err = GetObject[item](c, origin, 10, 2)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(item3, object2)

	// Commit changes

	requireT.NoError(c.Commit())

	// Create new cache and verify that objects exist

	c, err = cache.New(s, cacheSize)
	requireT.NoError(err)

	object1, exists, err = GetObject[item](c, origin, 10, 1)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(item1, object1)

	object2, exists, err = GetObject[item](c, origin, 10, 2)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(item3, object2)
}

func TestStoringBatches(t *testing.T) {
	const (
		nBatches        = 1200
		batchSize       = 5
		objectsPerBlock = 10
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
			requireT.NoError(SetObject[item](c, origin, nil, objectsPerBlock, blocks.ObjectID(k), item{
				Field1: k,
				Field2: 1,
			}))
		}

		// Get objects before committing

		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			object, exists, err := GetObject[item](c, origin, objectsPerBlock, blocks.ObjectID(k))
			requireT.NoError(err)
			requireT.True(exists)
			requireT.Equal(item{
				Field1: k,
				Field2: 1,
			}, object)
		}

		// Override objects before committing

		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			requireT.NoError(SetObject[item](c, origin, nil, objectsPerBlock, blocks.ObjectID(k), item{
				Field1: k,
				Field2: 2,
			}))
		}

		// Get objects before committing again

		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			object, exists, err := GetObject[item](c, origin, objectsPerBlock, blocks.ObjectID(k))
			requireT.NoError(err)
			requireT.True(exists)
			requireT.Equal(item{
				Field1: k,
				Field2: 2,
			}, object)
		}

		// Commit changes
		requireT.NoError(c.Commit())
	}

	// Get objects

	for k, i := 0, 0; i < nBatches; i++ {
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			object, exists, err := GetObject[item](c, origin, objectsPerBlock, blocks.ObjectID(k))
			requireT.NoError(err)
			requireT.True(exists)
			requireT.Equal(item{
				Field1: k,
				Field2: 2,
			}, object)
		}
	}

	// Create new cache

	c, err = cache.New(s, 15*blocks.BlockSize)
	requireT.NoError(err)

	// Get objects

	for k, i := 0, 0; i < nBatches; i++ {
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			object, exists, err := GetObject[item](c, origin, objectsPerBlock, blocks.ObjectID(k))
			requireT.NoError(err)
			requireT.True(exists)
			requireT.Equal(item{
				Field1: k,
				Field2: 2,
			}, object)
		}
	}
}
