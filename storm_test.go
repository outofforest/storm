package storm

import (
	"crypto/rand"
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
	Field uint64
}

func TestSetGet(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(devSize)
	requireT.NoError(persistence.Initialize(dev, false))

	s, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	c, err := cache.New(s, cacheSize)
	requireT.NoError(err)

	storm := New[item](c, blocks.SpaceID(0), 100)

	// Gte non-existing item

	_, exists, err := storm.Get([]byte{0x00, 0x01})
	requireT.NoError(err)
	requireT.False(exists)

	// Set

	requireT.NoError(storm.Set([]byte{0x00, 0x01}, item{Field: 1}))
	requireT.NoError(c.Commit())

	// Get same item

	itm, exists, err := storm.Get([]byte{0x00, 0x01})
	requireT.NoError(err)
	requireT.True(exists)
	requireT.EqualValues(1, itm.Field)

	// Set second item

	requireT.NoError(storm.Set([]byte{0x00, 0x02}, item{Field: 2}))

	// Get both items

	itm, exists, err = storm.Get([]byte{0x00, 0x01})
	requireT.NoError(err)
	requireT.True(exists)
	requireT.EqualValues(1, itm.Field)

	itm, exists, err = storm.Get([]byte{0x00, 0x02})
	requireT.NoError(err)
	requireT.True(exists)
	requireT.EqualValues(2, itm.Field)

	// Commit

	requireT.NoError(c.Commit())

	// Get items again

	itm, exists, err = storm.Get([]byte{0x00, 0x01})
	requireT.NoError(err)
	requireT.True(exists)
	requireT.EqualValues(1, itm.Field)

	itm, exists, err = storm.Get([]byte{0x00, 0x02})
	requireT.NoError(err)
	requireT.True(exists)
	requireT.EqualValues(2, itm.Field)

	// Update one item

	requireT.NoError(storm.Set([]byte{0x00, 0x02}, item{Field: 3}))
	requireT.NoError(c.Commit())

	// Get them again

	itm, exists, err = storm.Get([]byte{0x00, 0x01})
	requireT.NoError(err)
	requireT.True(exists)
	requireT.EqualValues(1, itm.Field)

	itm, exists, err = storm.Get([]byte{0x00, 0x02})
	requireT.NoError(err)
	requireT.True(exists)
	requireT.EqualValues(3, itm.Field)

	// Get from new cache

	c, err = cache.New(s, cacheSize)
	requireT.NoError(err)

	storm = New[item](c, blocks.SpaceID(0), 100)

	itm, exists, err = storm.Get([]byte{0x00, 0x01})
	requireT.NoError(err)
	requireT.True(exists)
	requireT.EqualValues(1, itm.Field)

	itm, exists, err = storm.Get([]byte{0x00, 0x02})
	requireT.NoError(err)
	requireT.True(exists)
	requireT.EqualValues(3, itm.Field)
}

func TestStoringBatches(t *testing.T) {
	const (
		nBatches  = 1000
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

	storm := New[item](c, blocks.SpaceID(0), 100)

	// Create keys
	keys := make([][48]byte, nBatches*batchSize)
	for i := 0; i < len(keys); i++ {
		_, err := rand.Read(keys[i][:])
		requireT.NoError(err)
	}

	for k, i := uint64(0), 0; i < nBatches; i++ {
		startK := k
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			requireT.NoError(storm.Set(keys[k][:], item{Field: k}))
		}

		// Set again
		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			requireT.NoError(storm.Set(keys[k][:], item{Field: k}))
		}

		// Get items before committing

		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			itm, exists, err := storm.Get(keys[k][:])
			requireT.NoError(err)
			requireT.True(exists)
			requireT.Equal(k, itm.Field)
		}

		// Commit changes
		requireT.NoError(c.Commit())

		// Get items after committing

		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			itm, exists, err := storm.Get(keys[k][:])
			requireT.NoError(err)
			requireT.True(exists)
			requireT.Equal(k, itm.Field)
		}
	}

	// Get items

	for i := 0; i < len(keys); i++ {
		itm, exists, err := storm.Get(keys[i][:])
		requireT.NoError(err)
		requireT.True(exists)
		requireT.EqualValues(i, itm.Field)
	}

	// Create new cache

	c, err = cache.New(s, 15*blocks.BlockSize)
	requireT.NoError(err)

	storm = New[item](c, blocks.SpaceID(0), 100)

	// Get items

	for i := 0; i < len(keys); i++ {
		itm, exists, err := storm.Get(keys[i][:])
		requireT.NoError(err)
		requireT.True(exists)
		requireT.EqualValues(i, itm.Field)
	}
}

func TestCreatingLotsOfSpaces(t *testing.T) {
	const nSpaces = 5000

	requireT := require.New(t)

	dev := memdev.New(1024 * 1024 * 1024)
	requireT.NoError(persistence.Initialize(dev, false))

	s, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	// Cache is intentionally small to ensure that it behaves correctly when cached blocks are unloaded.
	c, err := cache.New(s, 15*blocks.BlockSize)
	requireT.NoError(err)

	// Set items

	for i := 0; i < nSpaces; i++ {
		storm := New[item](c, blocks.SpaceID(i), 100)
		requireT.NoError(storm.Set([]byte{0x00}, item{Field: uint64(i)}))
	}

	// Get items before committing

	for i := 0; i < nSpaces; i++ {
		storm := New[item](c, blocks.SpaceID(i), 100)
		itm, exists, err := storm.Get([]byte{0x00})
		requireT.NoError(err)
		requireT.True(exists)
		requireT.EqualValues(i, itm.Field)
	}

	// Commit changes
	requireT.NoError(c.Commit())

	// Get items after committing

	for i := 0; i < nSpaces; i++ {
		storm := New[item](c, blocks.SpaceID(i), 100)
		itm, exists, err := storm.Get([]byte{0x00})
		requireT.NoError(err)
		requireT.True(exists)
		requireT.EqualValues(i, itm.Field)
	}

	// Create new cache

	c, err = cache.New(s, 15*blocks.BlockSize)
	requireT.NoError(err)

	// Get items

	for i := 0; i < nSpaces; i++ {
		storm := New[item](c, blocks.SpaceID(i), 100)
		itm, exists, err := storm.Get([]byte{0x00})
		requireT.NoError(err)
		requireT.True(exists)
		requireT.EqualValues(i, itm.Field)
	}
}
