package persistence

import (
	"testing"

	"github.com/outofforest/photon"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/singularity"
	"github.com/outofforest/storm/pkg/memdev"
)

const size = 1024 * 1024 * 10 // 10MiB

func TestValidInitialization(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(size)
	requireT.NoError(Initialize(dev, false))

	_, err := OpenStore(dev)
	requireT.NoError(err)
}

func TestInvalidChecksum(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(size)
	requireT.NoError(Initialize(dev, false))

	store, err := OpenStore(dev)
	requireT.NoError(err)

	// Set invalid checksum

	sBlock := photon.NewFromValue(&singularity.Block{})
	requireT.NoError(store.ReadBlock(0, sBlock.B))

	sBlock.V.Checksum = 0
	requireT.NoError(store.WriteBlock(0, sBlock.B))
	requireT.NoError(store.Sync())

	// Opening new store should fail

	_, err = OpenStore(dev)
	requireT.Error(err)
}

func TestInvalidBlockNumber(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(size)
	requireT.NoError(Initialize(dev, false))

	store, err := OpenStore(dev)
	requireT.NoError(err)

	// Set invalid number of blocks

	sBlock := photon.NewFromValue(&singularity.Block{})
	requireT.NoError(store.ReadBlock(0, sBlock.B))

	sBlock.V.NBlocks++
	sBlock.V.Checksum = blocks.BlockChecksum(sBlock.V)
	requireT.NoError(store.WriteBlock(0, sBlock.B))
	requireT.NoError(store.Sync())

	// Opening new store should fail

	_, err = OpenStore(dev)
	requireT.Error(err)
}

func TestExpandingDevWorks(t *testing.T) {
	requireT := require.New(t)

	dev := memdev.New(size)
	requireT.NoError(Initialize(dev, false))

	store, err := OpenStore(dev)
	requireT.NoError(err)

	// Set lower number of blocks to simulate device being expanded

	sBlock := photon.NewFromValue(&singularity.Block{})
	requireT.NoError(store.ReadBlock(0, sBlock.B))

	sBlock.V.NBlocks--
	sBlock.V.Checksum = 0
	sBlock.V.Checksum = blocks.BlockChecksum(sBlock.V)
	requireT.NoError(store.WriteBlock(0, sBlock.B))
	requireT.NoError(store.Sync())

	// Opening new store should succeed

	_, err = OpenStore(dev)
	requireT.NoError(err)
}
