package keystore

import (
	"github.com/outofforest/storm/blocks"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
	"github.com/outofforest/storm/cache"
)

// Trace stores the trace of incremented pointer blocks leading to the final leaf node
type Trace[T blocks.Block] struct {
	PointerBlocks []cache.Block[pointerV0.Block]
	Block         cache.Block[T]
}
