package storm

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"

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

	store, err := New(dev, cacheSize)
	requireT.NoError(err)

	var key [32]byte
	_, err = rand.Read(key[:])
	requireT.NoError(err)
	var value [32]byte
	_, err = rand.Read(value[:])
	requireT.NoError(err)

	// Set

	requireT.NoError(store.Set(key, value))
	requireT.NoError(store.Commit())

	// Get from the same cache

	value2, exists, err := store.Get(key)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(value, value2)

	// Get from new cache

	store2, err := New(dev, cacheSize)
	requireT.NoError(err)

	value2, exists, err = store2.Get(key)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Equal(value, value2)
}
