package storm

// BlockSize is the size of the data unit used by storm.
const BlockSize = 128 * 1024 // 128 KiB

// BlockBytes represents the raw data bytes of the block.
type BlockBytes [BlockSize]byte

// SingularityBlock is the starting block of the store. Everything starts and ends here.
type SingularityBlock struct {
}

// PointerBlock is the block forming tree. It contains pointers to other blocks.
type PointerBlock struct {
}

// DataBlock contains key-value pairs.
type DataBlock struct {
}
