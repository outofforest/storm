package objectstore

import (
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

func BenchmarkObjectStore(b *testing.B) {
	const (
		size            = 30000
		objectsPerBlock = 100
	)

	b.StopTimer()
	b.ResetTimer()

	requireT := require.New(b)

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

		sBlock := c.SingularityBlock()
		origin := cache.BlockOrigin{
			Pointer:   &sBlock.RootObjects,
			BlockType: &sBlock.RootObjectsBlockType,
		}

		b.StartTimer()
		func() {
			for i := 0; i < size; i++ {
				_ = SetObject[item](c, origin, objectsPerBlock, blocks.ObjectID(i), item{Field1: 1, Field2: 2})
			}

			_ = c.Commit()
		}()

		func() {
			for i := 0; i < size; i++ {
				_, _, _ = GetObject[item](c, origin, objectsPerBlock, blocks.ObjectID(i))
			}
		}()
		b.StopTimer()
	}
}
