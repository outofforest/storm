package keystore

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/cache"
	"github.com/outofforest/storm/persistence"
	"github.com/outofforest/storm/pkg/memdev"
)

// go test -bench=. -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

func BenchmarkKeystore(b *testing.B) {
	requireT := require.New(b)

	dev := memdev.New(300 * 1024 * 1024)
	requireT.NoError(persistence.Initialize(dev, false))

	s, err := persistence.OpenStore(dev)
	requireT.NoError(err)

	c, err := cache.New(s, 300*1024*1024)
	requireT.NoError(err)

	store, err := New(c)
	requireT.NoError(err)

	keys := make([][48]byte, 1000)

	for i := 0; i < len(keys); i++ {
		_, err := rand.Read(keys[i][:])
		requireT.NoError(err)
	}

	b.ResetTimer()
	for i := 0; i < len(keys); i++ {
		_, err := store.EnsureObjectID(keys[i][:])
		requireT.NoError(err)
	}

	requireT.NoError(c.Commit())
	b.StopTimer()
}
