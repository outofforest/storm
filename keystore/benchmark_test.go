package keystore

import (
	"crypto/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/cache"
	"github.com/outofforest/storm/persistence"
	"github.com/outofforest/storm/pkg/filedev"
)

// go test -bench=. -run=^$ -cpuprofile profile.out -benchtime=5x
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

func BenchmarkKeyStore(b *testing.B) {
	const size = 30000

	b.StopTimer()
	b.ResetTimer()

	requireT := require.New(b)

	keys := make([][48]byte, size)

	for i := 0; i < len(keys); i++ {
		_, err := rand.Read(keys[i][:])
		requireT.NoError(err)
	}

	f, err := os.OpenFile("/home/wojciech/testdev", os.O_RDWR, 0o600)
	requireT.NoError(err)
	defer f.Close()

	// dev := memdev.New(300 * 1024 * 1024)
	dev := filedev.New(f)
	for bi := 0; bi < b.N; bi++ {
		requireT.NoError(persistence.Initialize(dev, true))

		s, err := persistence.OpenStore(dev)
		requireT.NoError(err)

		c, err := cache.New(s, 500*1024*1024)
		requireT.NoError(err)

		var blockType blocks.BlockType
		origin := cache.BlockOrigin{
			Pointer:   &blocks.Pointer{},
			BlockType: &blockType,
		}
		var objectIndex blocks.ObjectID

		b.StartTimer()
		func() {
			for i := 0; i < len(keys); i++ {
				_, _ = EnsureObjectID(c, origin, nil, &objectIndex, keys[i][:])
			}

			_ = c.Commit()
		}()

		func() {
			for i := 0; i < len(keys); i++ {
				_, _, _ = GetObjectID(c, origin, keys[i][:])
			}
		}()
		b.StopTimer()
	}
}
