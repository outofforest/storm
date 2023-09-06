package keystore

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/blocks"
	objectlistV0 "github.com/outofforest/storm/blocks/objectlist/v0"
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

	store, err := New(c)
	requireT.NoError(err)

	// Key intentionally takes 2.5 chunks.
	key := []byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
		0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
		0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f,
		0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f,
	}

	// Set

	objectID, err := store.EnsureObjectID(key)
	requireT.NoError(err)
	requireT.NoError(c.Commit())

	// Get from the same cache

	objectID2, exists, err := store.GetObjectID(key)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(objectID, objectID2)

	// Get from new cache

	c2, err := cache.New(s, cacheSize)
	requireT.NoError(err)

	store2, err := New(c2)
	requireT.NoError(err)

	objectID2, exists, err = store2.GetObjectID(key)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(objectID, objectID2)
}

func TestStoringBatches(t *testing.T) {
	const (
		nBatches  = 30
		batchSize = 50 * objectlistV0.ChunksPerBlock
	)

	requireT := require.New(t)

	dev := memdev.New(2048 * 1024 * 1024)
	requireT.NoError(persistence.Initialize(dev, false))

	s, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	// Cache is intentionally small to ensure that it behaves correctly when cached blocks are unloaded.
	c, err := cache.New(s, 15*blocks.BlockSize)
	requireT.NoError(err)

	store, err := New(c)
	requireT.NoError(err)

	keys := make([][48]byte, nBatches*batchSize)

	// Create key-objectID pairs

	for i := 0; i < len(keys); i++ {
		_, err := rand.Read(keys[i][:])
		requireT.NoError(err)
	}
	objectIDs := make([]blocks.ObjectID, 0, len(keys))

	for k, i := 0, 0; i < nBatches; i++ {
		startK := k
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			objectID, err := store.EnsureObjectID(keys[k][:])
			requireT.NoError(err)
			objectIDs = append(objectIDs, objectID)
		}

		// Verify that ensured object ID is the same before committing.
		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			objectID, err := store.EnsureObjectID(keys[k][:])
			requireT.NoError(err)
			requireT.Equal(objectIDs[k], objectID)
		}

		// Get object IDs before committing

		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			objectID, exists, err := store.GetObjectID(keys[k][:])
			requireT.NoError(err)
			requireT.True(exists)
			requireT.Equal(objectIDs[k], objectID)
		}

		// Commit changes
		requireT.NoError(c.Commit())

		// Verify that next time same object ID is returned.
		// As a side effect, it triggers references to pointer blocks to be incremented without bing committed.
		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			objectID, err := store.EnsureObjectID(keys[k][:])
			requireT.NoError(err)
			requireT.Equal(objectIDs[k], objectID)
		}

		// Get object IDs

		k = startK
		for j := 0; j < batchSize; j, k = j+1, k+1 {
			objectID, exists, err := store.GetObjectID(keys[k][:])
			requireT.NoError(err)
			requireT.True(exists)
			requireT.Equal(objectIDs[k], objectID)
		}
	}

	// Get object IDs

	for i := 0; i < len(keys); i++ {
		objectID, exists, err := store.GetObjectID(keys[i][:])
		requireT.NoError(err)
		requireT.True(exists)
		requireT.Equal(objectIDs[i], objectID)
	}

	// If the same key is ensured again, same ID should be returned

	for i := 0; i < len(keys); i++ {
		objectID, err := store.EnsureObjectID(keys[i][:])
		requireT.NoError(err)
		requireT.Equal(objectIDs[i], objectID)
	}

	// Create new cache

	c, err = cache.New(s, 15*blocks.BlockSize)
	requireT.NoError(err)

	store, err = New(c)
	requireT.NoError(err)

	// Get object IDs

	for i := 0; i < len(keys); i++ {
		objectID, exists, err := store.GetObjectID(keys[i][:])
		requireT.NoError(err)
		requireT.True(exists)
		requireT.Equal(objectIDs[i], objectID)
	}

	// If the same key is ensured, same ID should be returned

	for i := 0; i < len(keys); i++ {
		objectID, err := store.EnsureObjectID(keys[i][:])
		requireT.NoError(err)
		requireT.Equal(objectIDs[i], objectID)
	}
}
