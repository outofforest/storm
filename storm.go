package storm

import (
	"bytes"
	"encoding/hex"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/blocks"
	objectlistV0 "github.com/outofforest/storm/blocks/objectlist/v0"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
	singularityV0 "github.com/outofforest/storm/blocks/singularity/v0"
	"github.com/outofforest/storm/cache"
	"github.com/outofforest/storm/persistence"
)

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
	if len(key) > objectlistV0.MaxKeyComponentLength {
		return 0, false, errors.Errorf("maximum key component length exceeded, maximum: %d, actual: %d", objectlistV0.MaxKeyComponentLength, len(key))
	}

	sBlock := s.c.SingularityBlock()
	dataBlockPath, hashReminder, exists, err := lookupByKeyHash[objectlistV0.Block](
		s.c,
		&sBlock.RootData,
		&sBlock.RootDataBlockType,
		&sBlock.RootDataSchemaVersion,
		blocks.ObjectListV0,
		false,
		xxhash.Sum64(key),
	)
	if !exists || err != nil {
		return 0, false, err
	}

	block := &dataBlockPath.Leaf.Block
	index, chunkFound := findChunkForKey(block, key, hashReminder)
	if chunkFound && block.ChunkPointerStates[index] == objectlistV0.DefinedChunkState {
		return block.ObjectLinks[index], true, nil
	}
	return 0, false, nil
}

// Set sets value for a key in the store.
func (s *Storm) Set(key []byte, objectID blocks.ObjectID) error {
	if len(key) == 0 {
		return errors.Errorf("key cannot be empty")
	}
	if len(key) > objectlistV0.MaxKeyComponentLength {
		return errors.Errorf("maximum key component length exceeded, maximum: %d, actual: %d", objectlistV0.MaxKeyComponentLength, len(key))
	}

	sBlock := s.c.SingularityBlock()
	dataBlockPath, hashReminder, _, err := lookupByKeyHash[objectlistV0.Block](
		s.c,
		&sBlock.RootData,
		&sBlock.RootDataBlockType,
		&sBlock.RootDataSchemaVersion,
		blocks.ObjectListV0,
		true,
		xxhash.Sum64(key),
	)
	if err != nil {
		return err
	}

	// TODO (wojciech): Implement better open addressing

	block := &dataBlockPath.Leaf.Block

	//nolint:staticcheck
	if block.NUsedItems >= objectlistV0.SplitTrigger {
		// TODO (wojciech): Implement block splitting
	}

	if err := setObjectID(block, key, hashReminder, objectID); err != nil {
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

func verifyKey(key []byte, hashReminder uint64, block *objectlistV0.Block, index uint16) bool {
	if block.KeyHashReminders[index] != hashReminder {
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

//nolint:unused
func ensureObjectID(
	sBlock *singularityV0.Block,
	block *objectlistV0.Block,
	key []byte,
	hashReminder uint64,
) (blocks.ObjectID, bool, error) {
	index, chunkFound := findChunkForKey(block, key, hashReminder)
	if !chunkFound {
		return 0, false, errors.Errorf("cannot find chunk for key %s", hex.EncodeToString(key))
	}

	if block.ChunkPointerStates[index] == objectlistV0.DefinedChunkState {
		return block.ObjectLinks[index], false, nil
	}
	if err := setupChunk(block, index, key, hashReminder); err != nil {
		return 0, false, err
	}

	block.ObjectLinks[index] = sBlock.NextObjectID
	sBlock.NextObjectID++

	return block.ObjectLinks[index], true, nil
}

func setObjectID(
	block *objectlistV0.Block,
	key []byte,
	hashReminder uint64,
	objectID blocks.ObjectID,
) error {
	index, chunkFound := findChunkForKey(block, key, hashReminder)
	if !chunkFound {
		return errors.Errorf("cannot find chunk for key %s", hex.EncodeToString(key))
	}

	// If state is `DefinedChunkState` it means that key is already set in the right chunk
	// and only new object ID must be set.
	if block.ChunkPointerStates[index] != objectlistV0.DefinedChunkState {
		if err := setupChunk(block, index, key, hashReminder); err != nil {
			return err
		}
	}
	block.ObjectLinks[index] = objectID

	return nil
}

func setupChunk(
	block *objectlistV0.Block,
	index uint16,
	key []byte,
	hashReminder uint64,
) error {
	nBlocksRequired := (uint16(len(key)) + objectlistV0.ChunkSize - 1) / objectlistV0.ChunkSize
	if block.NUsedItems+nBlocksRequired > objectlistV0.ChunksPerBlock {
		// At this point, if block hasn't been split before, it means that all the chunks
		// are taken by keys producing the same hash, meaning that it's not possible to set the key.
		return errors.New("block does not contain enough free chunks to add new key")
	}

	if block.NUsedItems == 0 {
		// In this case the list of free blocks must be initialized.
		block.FreeChunkIndex = 0
		for i := uint16(0); i < objectlistV0.ChunksPerBlock; i++ {
			block.NextChunkPointers[i] = i + 1
		}
	}

	block.ChunkPointerStates[index] = objectlistV0.DefinedChunkState
	block.ChunkPointers[index] = block.FreeChunkIndex
	block.KeyHashReminders[index] = hashReminder

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

	return nil
}

func findChunkForKey(
	block *objectlistV0.Block,
	key []byte,
	hashReminder uint64,
) (uint16, bool) {
	var invalidChunkFound bool
	var invalidChunkIndex uint16
	for i, index := 0, uint16(hashReminder%objectlistV0.ChunksPerBlock); i < objectlistV0.ChunksPerBlock; i, index = i+1, (index+1)%objectlistV0.ChunksPerBlock {
		switch block.ChunkPointerStates[index] {
		case objectlistV0.DefinedChunkState:
			if !verifyKey(key, hashReminder, block, index) {
				continue
			}
		case objectlistV0.InvalidChunkState:
			if !invalidChunkFound {
				invalidChunkFound = true
				invalidChunkIndex = index
			}
			continue
		}

		if block.ChunkPointerStates[index] == objectlistV0.FreeChunkState && invalidChunkFound {
			return invalidChunkIndex, true
		}

		return index, true
	}

	return 0, false
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

func lookupByKeyHash[T blocks.Block](
	c *cache.Cache,
	rootPointer *pointerV0.Pointer,
	rootBlockType *blocks.BlockType,
	rootBlockSchemaVersion *blocks.SchemaVersion,
	leafSchemaVersion blocks.SchemaVersion,
	createIfMissing bool,
	keyHash uint64,
) (keyPath[T], uint64, bool, error) {
	currentPointer := *rootPointer
	currentPointedBlockType := *rootBlockType

	hashReminder := keyHash
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
				}, hashReminder, true, nil
			}
			return keyPath[T]{}, 0, false, nil
		case blocks.LeafBlockType:
			leafBlock, err := cache.FetchBlock[T](c, currentPointer)
			if err != nil {
				return keyPath[T]{}, 0, false, err
			}
			return keyPath[T]{
				rootPointer:            rootPointer,
				rootBlockType:          rootBlockType,
				rootBlockSchemaVersion: rootBlockSchemaVersion,
				leafSchemaVersion:      leafSchemaVersion,
				hops:                   hops,
				Leaf:                   leafBlock,
			}, hashReminder, true, nil
		case blocks.PointerBlockType:
			pointerBlock, err := cache.FetchBlock[pointerV0.Block](c, currentPointer)
			if err != nil {
				return keyPath[T]{}, 0, false, err
			}

			pointerIndex := hashReminder % pointerV0.PointersPerBlock
			hashReminder /= pointerV0.PointersPerBlock

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
