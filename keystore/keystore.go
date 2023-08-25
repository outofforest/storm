package keystore

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
)

// Store represents the key store keeping the relation between keys and object IDs.
type Store struct {
	c *cache.Cache
}

// New returns new key store.
func New(c *cache.Cache) (*Store, error) {
	return &Store{
		c: c,
	}, nil
}

// GetObjectID returns existing object ID for key.
func (s *Store) GetObjectID(key []byte) (blocks.ObjectID, bool, error) {
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

// EnsureObjectID returns object ID for key. If the object ID does not exist it is created.
func (s *Store) EnsureObjectID(key []byte) (blocks.ObjectID, error) {
	if len(key) == 0 {
		return 0, errors.Errorf("key cannot be empty")
	}
	if len(key) > objectlistV0.MaxKeyComponentLength {
		return 0, errors.Errorf("maximum key component length exceeded, maximum: %d, actual: %d", objectlistV0.MaxKeyComponentLength, len(key))
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
		return 0, err
	}

	// TODO (wojciech): Implement better open addressing

	if dataBlockPath.Leaf.Block.NUsedChunks >= objectlistV0.SplitTrigger {
		// TODO (wojciech): Check if split is possible - if all the keys have the same hash then it is not.
		pointerBlock, leafBlock, leafSchemaVersion, err := splitBlock(&dataBlockPath.Leaf.Block, hashReminder, s.c)
		if err != nil {
			return 0, err
		}
		dataBlockPath = dataBlockPath.Split(pointerBlock, uint16(hashReminder%pointerV0.PointersPerBlock), leafBlock, leafSchemaVersion)
		hashReminder /= pointerV0.PointersPerBlock
	}

	objectID, created, err := ensureObjectID(sBlock, &dataBlockPath.Leaf.Block, key, hashReminder)
	if err != nil {
		return 0, err
	}
	if created {
		if _, err := dataBlockPath.Commit(); err != nil {
			return 0, err
		}
	}
	return objectID, nil
}

// Delete deletes key from the store.
func (s *Store) Delete(key [32]byte) error {
	// TODO (wojciech): To be implemented
	return nil
}

func verifyKey(key []byte, hashReminder uint64, block *objectlistV0.Block, index uint16) bool {
	if block.KeyHashReminders[index] != hashReminder {
		return false
	}

	chunkIndex := block.ChunkPointers[index]
	for {
		chunkOffset := uint64(chunkIndex) * objectlistV0.ChunkSize
		nextChunkIndex := block.NextChunkPointers[chunkIndex]
		if nextChunkIndex > objectlistV0.ChunksPerBlock {
			// This is the last chunk in the sequence.
			remainingLength := uint64(nextChunkIndex) - objectlistV0.ChunksPerBlock
			return bytes.Equal(key, block.Blob[chunkOffset:chunkOffset+remainingLength])
		}

		if !bytes.Equal(key[:objectlistV0.ChunkSize], block.Blob[chunkOffset:chunkOffset+objectlistV0.ChunkSize]) {
			return false
		}
		key = key[objectlistV0.ChunkSize:]
		chunkIndex = nextChunkIndex
	}
}

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

func setupChunk(
	block *objectlistV0.Block,
	index uint16,
	key []byte,
	hashReminder uint64,
) error {
	nBlocksRequired := (uint16(len(key)) + objectlistV0.ChunkSize - 1) / objectlistV0.ChunkSize
	if block.NUsedChunks+nBlocksRequired > objectlistV0.ChunksPerBlock {
		// At this point, if block hasn't been split before, it means that all the chunks
		// are taken by keys producing the same hash, meaning that it's not possible to set the key.
		return errors.New("block does not contain enough free chunks to add new key")
	}

	if block.NUsedChunks == 0 {
		initObjectList(block)
	}

	block.ChunkPointerStates[index] = objectlistV0.DefinedChunkState
	block.ChunkPointers[index] = block.FreeChunkIndex
	block.KeyHashReminders[index] = hashReminder

	var lastChunkIndex uint16
	var nCopied int
	keyToCopy := key
	for len(keyToCopy) > 0 {
		block.NUsedChunks++
		lastChunkIndex = block.FreeChunkIndex
		block.FreeChunkIndex = block.NextChunkPointers[block.FreeChunkIndex]
		block.NextChunkPointers[lastChunkIndex] = block.FreeChunkIndex

		chunkOffset := uint64(lastChunkIndex) * objectlistV0.ChunkSize

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
			if key == nil || !verifyKey(key, hashReminder, block, index) {
				continue
			}
		case objectlistV0.InvalidChunkState:
			if !invalidChunkFound {
				invalidChunkFound = true
				invalidChunkIndex = index
			}
			continue
		case objectlistV0.FreeChunkState:
			if invalidChunkFound {
				return invalidChunkIndex, true
			}
		}
		return index, true
	}

	if invalidChunkFound {
		return invalidChunkIndex, true
	}

	return 0, false
}

func initObjectList(block *objectlistV0.Block) {
	for i := uint16(0); i < objectlistV0.ChunksPerBlock; i++ {
		block.NextChunkPointers[i] = i + 1
	}
}

func splitBlock(block *objectlistV0.Block, hashReminder uint64, c *cache.Cache) (cache.CachedBlock[pointerV0.Block], cache.CachedBlock[objectlistV0.Block], blocks.SchemaVersion, error) {
	// TODO (wojciech): Find a way to allocate only the amount of blocks really needed
	newPointerIndexes := make([]uint16, 0, pointerV0.PointersPerBlock)
	newCachedBlocks := make([]cache.CachedBlock[objectlistV0.Block], 0, pointerV0.PointersPerBlock)
	newBlocks := map[uint16]*objectlistV0.Block{}

	// Block representing the original hash reminder is created explicitly to be returned even if it's empty,
	// because later new key is inserted into it.
	returnedPointerIndex := uint16(hashReminder % pointerV0.PointersPerBlock)
	newPointerIndexes = append(newPointerIndexes, returnedPointerIndex)
	newCachedBlocks = append(newCachedBlocks, cache.NewBlock[objectlistV0.Block](c))
	returnedBlock := &newCachedBlocks[len(newCachedBlocks)-1].Block
	newBlocks[returnedPointerIndex] = returnedBlock
	initObjectList(returnedBlock)

	for i := uint16(0); i < objectlistV0.ChunksPerBlock; i++ {
		if block.ChunkPointerStates[i] != objectlistV0.DefinedChunkState {
			continue
		}
		pointerIndex := uint16(block.KeyHashReminders[i] % pointerV0.PointersPerBlock)
		newBlock, exists := newBlocks[pointerIndex]
		if !exists {
			newPointerIndexes = append(newPointerIndexes, pointerIndex)
			newCachedBlocks = append(newCachedBlocks, cache.NewBlock[objectlistV0.Block](c))
			newBlock = &newCachedBlocks[len(newCachedBlocks)-1].Block
			newBlocks[pointerIndex] = newBlock
			initObjectList(newBlock)
		}
		mergeChunkIntoBlock(newBlock, block, i)
	}

	pointerBlock := cache.NewBlock[pointerV0.Block](c)
	var returnedCachedBlock cache.CachedBlock[objectlistV0.Block]
	for i := 0; i < len(newCachedBlocks); i++ {
		pointerIndex := newPointerIndexes[i]

		if pointerIndex == returnedPointerIndex {
			returnedCachedBlock = newCachedBlocks[i]
			continue
		}

		pointerBlock.Block.NUsedPointers++
		pointerBlock.Block.PointedBlockTypes[pointerIndex] = blocks.LeafBlockType
		pointerBlock.Block.PointedBlockVersions[pointerIndex] = blocks.ObjectListV0

		cachedBlock, err := newCachedBlocks[i].Commit()
		if err != nil {
			return cache.CachedBlock[pointerV0.Block]{}, cache.CachedBlock[objectlistV0.Block]{}, 0, err
		}
		address, err := cachedBlock.Address()
		if err != nil {
			return cache.CachedBlock[pointerV0.Block]{}, cache.CachedBlock[objectlistV0.Block]{}, 0, err
		}

		pointerBlock.Block.Pointers[pointerIndex] = pointerV0.Pointer{
			Checksum: cachedBlock.Block.ComputeChecksum(),
			Address:  address,
		}
	}

	return pointerBlock, returnedCachedBlock, blocks.ObjectListV0, nil
}

func mergeChunkIntoBlock(dstBlock *objectlistV0.Block, srcBlock *objectlistV0.Block, srcIndex uint16) {
	objectID := srcBlock.ObjectLinks[srcIndex]

	srcHashReminder := srcBlock.KeyHashReminders[srcIndex]
	dstHashReminder := srcHashReminder / pointerV0.PointersPerBlock

	dstIndex, dstChunkFound := findChunkForKey(dstBlock, nil, dstHashReminder)
	if !dstChunkFound {
		panic("impossible situation")
	}

	dstBlock.ChunkPointerStates[dstIndex] = objectlistV0.DefinedChunkState
	dstBlock.ChunkPointers[dstIndex] = dstBlock.FreeChunkIndex
	dstBlock.ObjectLinks[dstIndex] = objectID
	dstBlock.KeyHashReminders[dstIndex] = dstHashReminder

	srcChunkIndex := srcBlock.ChunkPointers[srcIndex]
	var dstLastChunkIndex uint16
	for srcChunkIndex < objectlistV0.ChunksPerBlock {
		dstBlock.NUsedChunks++
		dstLastChunkIndex = dstBlock.FreeChunkIndex
		dstBlock.FreeChunkIndex = dstBlock.NextChunkPointers[dstBlock.FreeChunkIndex]
		dstBlock.NextChunkPointers[dstLastChunkIndex] = dstBlock.FreeChunkIndex

		srcChunkOffset := uint64(srcChunkIndex) * objectlistV0.ChunkSize
		dstChunkOffset := uint64(dstLastChunkIndex) * objectlistV0.ChunkSize

		copy(dstBlock.Blob[dstChunkOffset:dstChunkOffset+objectlistV0.ChunkSize], srcBlock.Blob[srcChunkOffset:srcChunkOffset+objectlistV0.ChunkSize])
		srcChunkIndex = srcBlock.NextChunkPointers[srcChunkIndex]
	}
	// For the last record `srcChunkIndex` contains `ChunkSize+remaininglength` so it might be assigned directly.
	dstBlock.NextChunkPointers[dstLastChunkIndex] = srcChunkIndex
}

type hop struct {
	Index uint16
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
	hops := make([]hop, 0, 11) // TODO (wojciech): Check size

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

			pointerIndex := uint16(hashReminder % pointerV0.PointersPerBlock)
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

func (kp keyPath[T]) Split(
	pointerBlock cache.CachedBlock[pointerV0.Block],
	pointerIndex uint16,
	leafBlock cache.CachedBlock[T],
	leafSchemaVersion blocks.SchemaVersion,
) keyPath[T] {
	kp.hops = append(kp.hops, hop{
		Block: pointerBlock,
		Index: pointerIndex,
	})
	kp.Leaf = leafBlock
	kp.leafSchemaVersion = leafSchemaVersion

	return kp
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
		lastHopBlock := &kp.hops[nHops-1].Block.Block
		lastHopIndex := kp.hops[nHops-1].Index
		if lastHopBlock.PointedBlockTypes[lastHopIndex] == blocks.FreeBlockType {
			lastHopBlock.NUsedPointers++
			lastHopBlock.PointedBlockTypes[lastHopIndex] = blocks.LeafBlockType
			lastHopBlock.PointedBlockVersions[lastHopIndex] = kp.leafSchemaVersion
		}
		for i := nHops - 1; i >= 0; i-- {
			hopBlock := &kp.hops[i].Block
			hopIndex := kp.hops[i].Index

			// TODO (wojciech): Clean up this
			if i != nHops-1 {
				hopBlock.Block.PointedBlockTypes[hopIndex] = blocks.PointerBlockType
				hopBlock.Block.PointedBlockVersions[hopIndex] = blocks.PointerV0
			}

			hopBlock.Block.Pointers[kp.hops[i].Index] = pointerV0.Pointer{
				Address:  address,
				Checksum: checksum,
			}
			pointerBlock, err := hopBlock.Commit()
			if err != nil {
				return cache.CachedBlock[T]{}, err
			}
			address, err = pointerBlock.Address()
			if err != nil {
				return cache.CachedBlock[T]{}, err
			}
			checksum = pointerBlock.Block.ComputeChecksum()
		}

		*kp.rootBlockType = blocks.PointerBlockType
		*kp.rootBlockSchemaVersion = blocks.PointerV0
	} else if *kp.rootBlockType == blocks.FreeBlockType {
		*kp.rootBlockType = blocks.LeafBlockType
		*kp.rootBlockSchemaVersion = kp.leafSchemaVersion
	}
	*kp.rootPointer = pointerV0.Pointer{
		Checksum: checksum,
		Address:  address,
	}

	return leaf, nil
}
