package spacelist

import "math/rand"

// AddressingOffsets are offsets used as open addressing scheme for chunks in the block.
var AddressingOffsets = func() []uint16 {
	addressingOffsets := make([]uint16, SpacesPerBlock)

	for i, v := range rand.New(rand.NewSource(0)).Perm(SpacesPerBlock) {
		addressingOffsets[i] = uint16(v)
	}

	return addressingOffsets
}()
