package keystore

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/cache"
	"github.com/outofforest/storm/persistence"
	"github.com/outofforest/storm/pkg/memdev"
)

// go test -bench=. -cpuprofile profile.out -benchtime=2x
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

func BenchmarkKeystore(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	requireT := require.New(b)

	keys := make([][48]byte, 30000)

	for i := 0; i < len(keys); i++ {
		_, err := rand.Read(keys[i][:])
		requireT.NoError(err)
	}

	// f, err := os.OpenFile("/home/wojciech/testdev", os.O_RDWR, 0o600)
	// requireT.NoError(err)
	// defer f.Close()

	dev := memdev.New(300 * 1024 * 1024)
	// dev := filedev.New(f)
	for bi := 0; bi < b.N; bi++ {
		requireT.NoError(persistence.Initialize(dev, true))

		s, err := persistence.OpenStore(dev)
		requireT.NoError(err)

		c, err := cache.New(s, 300*1024*1024)
		requireT.NoError(err)

		store, err := New(c)
		requireT.NoError(err)

		b.StartTimer()
		for i := 0; i < len(keys); i++ {
			_, err := store.EnsureObjectID(keys[i][:])
			requireT.NoError(err)
		}

		requireT.NoError(c.Commit())
		b.StopTimer()
	}
}
