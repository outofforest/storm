package cache

import (
	"fmt"
	"io"
	"testing"

	"github.com/outofforest/photon"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/storm/pkg/memdev"
	"github.com/outofforest/storm/types"
)

const (
	devSize   = 1024 * 1024 * 10 // 10MiB
	cacheSize = 1024 * 1024 * 5  // 5MiB
)

func TestCache(t *testing.T) {
	requireT := require.New(t)

	const pointerIndex = 3

	dev := memdev.New(devSize)
	newBlock := photon.NewFromValue(&types.PointerBlock{})
	newBlock.V.Pointers[pointerIndex].Address = 21

	_, err := dev.Seek(types.BlockSize, io.SeekStart)
	requireT.NoError(err)
	_, err = dev.Write(newBlock.B)
	requireT.NoError(err)

	cache := New(dev, cacheSize)

	block, err := GetBlock[types.PointerBlock](cache, 1)
	requireT.NoError(err)

	fmt.Printf("%#v\n", block.Block.V)

	requireT.Equal(types.BlockAddress(21), block.Block.V.Pointers[pointerIndex].Address)
}
