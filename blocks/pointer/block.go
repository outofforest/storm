package pointer

import (
	"github.com/outofforest/storm/blocks"
)

// TODO (wojciech): Hash data separately

// Block is the block forming tree. It contains pointers to other blocks.
type Block struct {
	Pointers          [PointersPerBlock]blocks.Pointer
	PointedBlockTypes [PointersPerBlock]blocks.BlockType
}
