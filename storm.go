package storm

import (
	"bytes"
	"encoding/hex"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/blocks"
	objectlistV0 "github.com/outofforest/storm/blocks/objectlist/v0"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
	"github.com/outofforest/storm/cache"
	"github.com/outofforest/storm/persistence"
)

// MaxKeyComponentLength specifies the maximum length of single key component.
const MaxKeyComponentLength = 256

// Storm represents the storm storage engine.
type Storm struct {
	c *cache.Cache
}

// New returns new storm store.
func New(dev persistence.Dev, cacheSize int64) (*Storm, error) {
	store, err := persistence.OpenStore(dev)
	if err != nil {
		return nil, err
	}
	c, err := cache.New(store, cacheSize)
	if err != nil {
		return nil, err
	}
	return &Storm{
		c: c,
	}, nil
}

// Get gets value for a key from the store.
func (s *Storm) Get(key []byte) (blocks.ObjectID, bool, error) {
	if len(key) == 0 {
		return 0, false, errors.Errorf("key cannot be empty")
	}
	if len(key) > MaxKeyComponentLength {
		return 0, false, errors.Errorf("maximum key component length exceeded, maximum: %d, actual: %d", MaxKeyComponentLength, len(key))
	}

	keyHash := xxhash.Sum64(key)
	sBlock := s.c.SingularityBlock()
	dataBlockPath, exists, err := lookupByKey[objectlistV0.Block](
		s.c,
		&sBlock.RootData,
		&sBlock.RootDataBlockType,
		&sBlock.RootDataSchemaVersion,
		blocks.ObjectListV0,
		false,
		keyHash,
	)
	if !exists || err != nil {
		return 0, false, err
	}

	block := &dataBlockPath.Leaf.Block
	for i, index := 0, uint16(keyHash%objectlistV0.ChunksPerBlock); i < objectlistV0.ChunksPerBlock; i, index = i+1, (index+1)%objectlistV0.ChunksPerBlock {
		if block.ChunkPointerStates[index] == objectlistV0.FreeChunkState {
			return 0, false, nil
		}
		if block.ChunkPointerStates[index] == objectlistV0.InvalidChunkState {
			continue
		}

		if verifyKey(key, keyHash, block, index) {
			return block.ObjectLinks[index], true, nil
		}
	}

	return 0, false, nil
}

// Set sets value for a key in the store.
func (s *Storm) Set(key []byte, objectID blocks.ObjectID) error {
	if len(key) == 0 {
		return errors.Errorf("key cannot be empty")
	}
	if len(key) > MaxKeyComponentLength {
		return errors.Errorf("maximum key component length exceeded, maximum: %d, actual: %d", MaxKeyComponentLength, len(key))
	}

	keyHash := xxhash.Sum64(key)
	sBlock := s.c.SingularityBlock()
	dataBlockPath, _, err := lookupByKey[objectlistV0.Block](
		s.c,
		&sBlock.RootData,
		&sBlock.RootDataBlockType,
		&sBlock.RootDataSchemaVersion,
		blocks.ObjectListV0,
		true,
		keyHash,
	)
	if err != nil {
		return err
	}

	// TODO (wojciech): Implement better open addressing
	// TODO (wojciech): Implement block splitting
	// TODO (wojciech): Split block even before it is full (if height is less than maximum)
	// TODO (wojciech): Splitting must be done in a way where it is guaranteed that at least one key of max length can be inserted

	block := &dataBlockPath.Leaf.Block
	if block.NUsedItems == 0 {
		// In this case the list of free blocks must be initialized.
		block.FreeChunkIndex = 0
		for i := uint16(0); i < objectlistV0.ChunksPerBlock; i++ {
			block.NextChunkPointers[i] = i + 1
		}
	}

	if err := setKeyInObjectList(block, key, keyHash, objectID); err != nil {
		return err
	}

	_, err = dataBlockPath.Commit()
	return err
}

// Delete deletes key from the store.
func (s *Storm) Delete(key [32]byte) error {
	// TODO (wojciech): To be implemented
	return nil
}

// Commit commits cached changes to the device.
func (s *Storm) Commit() error {
	return s.c.Commit()
}

func verifyKey(key []byte, keyHash uint64, block *objectlistV0.Block, index uint16) bool {
	if block.KeyHashes[index] != keyHash {
		return false
	}

	chunkIndex := block.ChunkPointers[index]
	for {
		chunkOffset := chunkIndex * objectlistV0.ChunkSize
		nextChunkIndex := block.NextChunkPointers[chunkIndex]
		if nextChunkIndex > objectlistV0.ChunksPerBlock {
			// This is the last chunk in the sequence.
			remainingLength := nextChunkIndex - objectlistV0.ChunksPerBlock
			return bytes.Equal(key, block.Blob[chunkOffset:chunkOffset+remainingLength])
		}

		if !bytes.Equal(key[:objectlistV0.ChunkSize], block.Blob[chunkOffset:chunkOffset+objectlistV0.ChunkSize]) {
			return false
		}
		key = key[objectlistV0.ChunkSize:]
		chunkIndex = nextChunkIndex
	}
}

func setKeyInObjectList(
	block *objectlistV0.Block,
	key []byte,
	keyHash uint64,
	objectID blocks.ObjectID,
) error {
	var invalidChunkFound bool
	var invalidChunkIndex uint16
	for i, index := 0, uint16(keyHash%objectlistV0.ChunksPerBlock); i < objectlistV0.ChunksPerBlock; i, index = i+1, (index+1)%objectlistV0.ChunksPerBlock {
		switch block.ChunkPointerStates[index] {
		case objectlistV0.DefinedChunkState:
			if !verifyKey(key, keyHash, block, index) {
				continue
			}
		case objectlistV0.InvalidChunkState:
			if !invalidChunkFound {
				invalidChunkFound = true
				invalidChunkIndex = index
			}
			continue
		}

		if block.ChunkPointerStates[index] == objectlistV0.FreeChunkState {
			nBlocksRequired := (uint16(len(key)) + objectlistV0.ChunkSize - 1) / objectlistV0.ChunkSize
			if block.NUsedItems+nBlocksRequired > objectlistV0.ChunksPerBlock {
				// At this point, if block hasn't been split before, it means that all the chunks
				// are taken by keys producing the same hash, meaning that it's not possible to set the key.
				return errors.New("block does not contain enough free chunks to add new key")
			}

			if invalidChunkFound {
				index = invalidChunkIndex
			}

			block.ChunkPointerStates[index] = objectlistV0.DefinedChunkState
			block.ChunkPointers[index] = block.FreeChunkIndex
			block.KeyHashes[index] = keyHash

			var lastChunkIndex uint16
			var nCopied int
			keyToCopy := key
			for len(keyToCopy) > 0 {
				block.NUsedItems++
				lastChunkIndex = block.FreeChunkIndex
				block.FreeChunkIndex = block.NextChunkPointers[block.FreeChunkIndex]

				// In case key is longer, this is the next chunk to use.
				// In the other case it is set to the same index after the loop finishes to indicate the end of the sequence.
				block.NextChunkPointers[lastChunkIndex] = block.FreeChunkIndex

				chunkOffset := lastChunkIndex * objectlistV0.ChunkSize

				nCopied = copy(block.Blob[chunkOffset:chunkOffset+objectlistV0.ChunkSize], keyToCopy)
				keyToCopy = keyToCopy[nCopied:]
			}
			// If pointer is higher than `ChunksPerBlock` it means that this is the last chunk in the sequence
			// and value (index - ChunksPerBlock) indicates the remaining length of the key to read from the last chunk.
			block.NextChunkPointers[lastChunkIndex] = uint16(objectlistV0.ChunksPerBlock + nCopied)
		}
		block.ObjectLinks[index] = objectID

		return nil
	}

	return errors.Errorf("cannot find non-coliding chunk for key %s", hex.EncodeToString(key))
}

type hop struct {
	Index uint64
	Block cache.CachedBlock[pointerV0.Block]
}

type keyPath[T blocks.Block] struct {
	rootPointer            *pointerV0.Pointer
	rootBlockType          *blocks.BlockType
	rootBlockSchemaVersion *blocks.SchemaVersion
	leafSchemaVersion      blocks.SchemaVersion
	hops                   []hop
	Leaf                   cache.CachedBlock[T]
}

func lookupByKey[T blocks.Block](
	c *cache.Cache,
	rootPointer *pointerV0.Pointer,
	rootBlockType *blocks.BlockType,
	rootBlockSchemaVersion *blocks.SchemaVersion,
	leafSchemaVersion blocks.SchemaVersion,
	createIfMissing bool,
	keyHash uint64,
) (keyPath[T], bool, error) {
	currentPointer := *rootPointer
	currentPointedBlockType := *rootBlockType

	hopAddressing := keyHash
	hops := make([]hop, 0, 11)

	for {
		switch currentPointedBlockType {
		case blocks.FreeBlockType:
			if createIfMissing {
				return keyPath[T]{
					rootPointer:            rootPointer,
					rootBlockType:          rootBlockType,
					rootBlockSchemaVersion: rootBlockSchemaVersion,
					leafSchemaVersion:      leafSchemaVersion,
					hops:                   hops,
					Leaf:                   cache.NewBlock[T](c),
				}, true, nil
			}
			return keyPath[T]{}, false, nil
		case blocks.LeafBlockType:
			leafBlock, err := cache.FetchBlock[T](c, currentPointer)
			if err != nil {
				return keyPath[T]{}, false, err
			}
			return keyPath[T]{
				rootPointer:            rootPointer,
				rootBlockType:          rootBlockType,
				rootBlockSchemaVersion: rootBlockSchemaVersion,
				leafSchemaVersion:      leafSchemaVersion,
				hops:                   hops,
				Leaf:                   leafBlock,
			}, true, nil
		case blocks.PointerBlockType:
			pointerBlock, err := cache.FetchBlock[pointerV0.Block](c, currentPointer)
			if err != nil {
				return keyPath[T]{}, false, err
			}

			pointerIndex := hopAddressing % pointerV0.PointersPerBlock
			hopAddressing /= pointerV0.PointersPerBlock

			currentPointedBlockType = pointerBlock.Block.PointedBlockTypes[pointerIndex]
			currentPointer = pointerBlock.Block.Pointers[pointerIndex]

			hops = append(hops, hop{
				Block: pointerBlock,
				Index: pointerIndex,
			})
		}
	}
}

func (kp keyPath[T]) Commit() (cache.CachedBlock[T], error) {
	leaf, err := kp.Leaf.Commit()
	if err != nil {
		return cache.CachedBlock[T]{}, err
	}
	address, err := leaf.Address()
	if err != nil {
		return cache.CachedBlock[T]{}, err
	}
	checksum := leaf.Block.ComputeChecksum()

	if nHops := len(kp.hops); nHops > 0 {
		lastHop := kp.hops[nHops-1]
		if lastHop.Block.Block.PointedBlockTypes[lastHop.Index] == blocks.FreeBlockType {
			lastHop.Block.Block.NUsedPointers++
			lastHop.Block.Block.PointedBlockTypes[lastHop.Index] = blocks.LeafBlockType
			lastHop.Block.Block.PointedBlockVersions[lastHop.Index] = kp.leafSchemaVersion
		}
		for i := nHops - 1; i >= 0; i-- {
			hop := kp.hops[i]
			hop.Block.Block.Pointers[hop.Index] = pointerV0.Pointer{
				Address:  address,
				Checksum: checksum,
			}
			pointerBlock, err := hop.Block.Commit()
			if err != nil {
				return cache.CachedBlock[T]{}, err
			}
			address, err = pointerBlock.Address()
			if err != nil {
				return cache.CachedBlock[T]{}, err
			}
			checksum = leaf.Block.ComputeChecksum()
		}
	}

	if *kp.rootBlockType == blocks.FreeBlockType {
		*kp.rootBlockType = blocks.LeafBlockType
		*kp.rootBlockSchemaVersion = kp.leafSchemaVersion
	}
	*kp.rootPointer = pointerV0.Pointer{
		Checksum: checksum,
		Address:  address,
	}

	return leaf, nil
}
