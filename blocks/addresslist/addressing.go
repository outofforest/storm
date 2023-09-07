package addresslist

import "math/rand"

// AddressingOffsets are offsets used as open addressing scheme for chunks in the block.
var AddressingOffsets = func() []uint16 {
	addressingOffsets := make([]uint16, SlotsPerBlock)

	for i, v := range rand.New(rand.NewSource(0)).Perm(SlotsPerBlock) {
		addressingOffsets[i] = uint16(v)
	}

	return addressingOffsets
}()
