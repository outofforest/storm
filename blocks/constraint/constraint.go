package constraint

import (
	"github.com/outofforest/storm/blocks"
	dataV0 "github.com/outofforest/storm/blocks/data/v0"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
	singularityV0 "github.com/outofforest/storm/blocks/singularity/v0"
)

// Block defines the constraint for generics using block types.
type Block interface {
	singularityV0.Block | pointerV0.Block | dataV0.Block
	ComputeChecksum() blocks.Hash
}
