package v0

import "math/rand"

// AddressingOffsets are offsets used as open addressing scheme for chunks in the block.
var AddressingOffsets = func() []uint16 {
	addressingOffsets := make([]uint16, ChunksPerBlock)

	for i, v := range rand.New(rand.NewSource(0)).Perm(ChunksPerBlock) {
		addressingOffsets[i] = uint16(v)
	}

	return addressingOffsets
}()
