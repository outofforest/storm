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
	dataBlock, hashReminder, exists, _, err := lookupByKeyHash(
		s.c,
		&sBlock.RootData,
		&sBlock.RootDataBlockType,
		&sBlock.RootDataSchemaVersion,
		false,
		xxhash.Sum64(key),
	)
	if !exists || err != nil {
		return 0, false, err
	}

	index, chunkFound := findChunkForKey(dataBlock.Block, key, hashReminder)
	if chunkFound && dataBlock.Block.ChunkPointerStates[index] == objectlistV0.DefinedChunkState {
		return dataBlock.Block.ObjectLinks[index], true, nil
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
	dataBlock, hashReminder, _, splitFunc, err := lookupByKeyHash(
		s.c,
		&sBlock.RootData,
		&sBlock.RootDataBlockType,
		&sBlock.RootDataSchemaVersion,
		true,
		xxhash.Sum64(key),
	)
	if err != nil {
		return 0, err
	}

	// TODO (wojciech): Implement better open addressing

	return s.ensureObjectID(sBlock, dataBlock, key, hashReminder, splitFunc)
}

// Delete deletes key from the store.
func (s *Store) Delete(key [32]byte) error {
	// TODO (wojciech): To be implemented
	return nil
}

func (s *Store) ensureObjectID(
	sBlock *singularityV0.Block,
	block cache.Block[objectlistV0.Block],
	key []byte,
	hashReminder uint64,
	splitFunc func() (cache.Block[pointerV0.Block], error),
) (blocks.ObjectID, error) {
	index, chunkFound := findChunkForKey(block.Block, key, hashReminder)
	if chunkFound && block.Block.ChunkPointerStates[index] == objectlistV0.DefinedChunkState {
		return block.Block.ObjectLinks[index], nil
	}

	if block.Block.NUsedChunks >= objectlistV0.SplitTrigger {
		// TODO (wojciech): Check if split is possible - if all the keys have the same hash then it is not.

		oldBlock := *block.Block
		cache.InvalidateBlock(s.c, block)

		newPointerBlock, err := splitFunc()
		if err != nil {
			return 0, err
		}

		block, err = s.splitBlock(oldBlock, newPointerBlock, hashReminder)
		if err != nil {
			return 0, err
		}

		hashReminder /= pointerV0.PointersPerBlock
		index, chunkFound = findChunkForKey(block.Block, key, hashReminder)
	}

	if !chunkFound {
		return 0, errors.Errorf("cannot find chunk for key %s", hex.EncodeToString(key))
	}

	if err := setupChunk(block.Block, index, key, hashReminder); err != nil {
		return 0, err
	}

	block.Block.ObjectLinks[index] = sBlock.NextObjectID
	if err := cache.DirtyBlock(s.c, block); err != nil {
		return 0, err
	}

	sBlock.NextObjectID++

	return block.Block.ObjectLinks[index], nil
}

func (s *Store) splitBlock(block objectlistV0.Block, newPointerBlock cache.Block[pointerV0.Block], hashReminder uint64) (cache.Block[objectlistV0.Block], error) {
	sBlock := s.c.SingularityBlock()

	newPointerBlock.IncrementReferences()
	returnedBlock, err := cache.NewBlock[objectlistV0.Block](s.c)
	if err != nil {
		return cache.Block[objectlistV0.Block]{}, err
	}

	newPointerBlock.Block.NUsedPointers = 1
	returnedPointerIndex := hashReminder % pointerV0.PointersPerBlock
	newPointerBlock.Block.Pointers[returnedPointerIndex] = pointerV0.Pointer{
		Address:       returnedBlock.Address(),
		BirthRevision: sBlock.Revision + 1,
	}
	newPointerBlock.Block.PointedBlockTypes[returnedPointerIndex] = blocks.LeafBlockType
	newPointerBlock.Block.PointedBlockVersions[returnedPointerIndex] = blocks.ObjectListV0

	returnedBlock.WithPostCommitFunc(newLeafBlockPostCommitFunc(
		s.c,
		newPointerBlock,
		&newPointerBlock.Block.Pointers[returnedPointerIndex],
		&newPointerBlock.Block.PointedBlockTypes[returnedPointerIndex],
		&newPointerBlock.Block.PointedBlockVersions[returnedPointerIndex],
		returnedBlock,
	))
	initObjectList(returnedBlock.Block)

	for i := uint16(0); i < objectlistV0.ChunksPerBlock; i++ {
		if block.ChunkPointerStates[i] != objectlistV0.DefinedChunkState {
			continue
		}

		var newBlock cache.Block[objectlistV0.Block]
		pointerIndex := block.KeyHashReminders[i] % pointerV0.PointersPerBlock
		if newPointerBlock.Block.PointedBlockTypes[pointerIndex] == blocks.FreeBlockType {
			newPointerBlock.IncrementReferences()
			var err error
			newBlock, err = cache.NewBlock[objectlistV0.Block](s.c)
			if err != nil {
				return cache.Block[objectlistV0.Block]{}, err
			}

			newBlock.WithPostCommitFunc(newLeafBlockPostCommitFunc(
				s.c,
				newPointerBlock,
				&newPointerBlock.Block.Pointers[pointerIndex],
				&newPointerBlock.Block.PointedBlockTypes[pointerIndex],
				&newPointerBlock.Block.PointedBlockVersions[pointerIndex],
				newBlock,
			))

			newPointerBlock.Block.NUsedPointers++
			newPointerBlock.Block.Pointers[pointerIndex] = pointerV0.Pointer{
				Address:       newBlock.Address(),
				BirthRevision: sBlock.Revision + 1,
			}
			newPointerBlock.Block.PointedBlockTypes[pointerIndex] = blocks.LeafBlockType
			newPointerBlock.Block.PointedBlockVersions[pointerIndex] = blocks.ObjectListV0

			initObjectList(newBlock.Block)
		} else {
			newPointerBlock.IncrementReferences()

			var addedToCache bool
			var err error
			newBlock, addedToCache, err = cache.FetchBlock[objectlistV0.Block](s.c, newPointerBlock.Block.Pointers[pointerIndex])
			if err != nil {
				return cache.Block[objectlistV0.Block]{}, err
			}

			if addedToCache {
				newBlock.WithPostCommitFunc(newLeafBlockPostCommitFunc(
					s.c,
					newPointerBlock,
					&newPointerBlock.Block.Pointers[pointerIndex],
					&newPointerBlock.Block.PointedBlockTypes[pointerIndex],
					&newPointerBlock.Block.PointedBlockVersions[pointerIndex],
					newBlock,
				))
			} else {
				newPointerBlock.DecrementReferences()
			}
		}

		mergeChunkIntoBlock(newBlock.Block, &block, i)
		if err := cache.DirtyBlock(s.c, newBlock); err != nil {
			return cache.Block[objectlistV0.Block]{}, err
		}
	}

	newPointerBlock.IncrementReferences()
	returnedBlock, addedToCache, err := cache.FetchBlock[objectlistV0.Block](s.c, newPointerBlock.Block.Pointers[returnedPointerIndex])
	if err != nil {
		return cache.Block[objectlistV0.Block]{}, err
	}

	if addedToCache {
		returnedBlock.WithPostCommitFunc(newLeafBlockPostCommitFunc(
			s.c,
			newPointerBlock,
			&newPointerBlock.Block.Pointers[returnedPointerIndex],
			&newPointerBlock.Block.PointedBlockTypes[returnedPointerIndex],
			&newPointerBlock.Block.PointedBlockVersions[returnedPointerIndex],
			returnedBlock,
		))
	} else {
		newPointerBlock.DecrementReferences()
	}

	return returnedBlock, nil
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

func mergeChunkIntoBlock(dstBlock *objectlistV0.Block, srcBlock *objectlistV0.Block, srcIndex uint16) {
	objectID := srcBlock.ObjectLinks[srcIndex]

	dstHashReminder := srcBlock.KeyHashReminders[srcIndex] / pointerV0.PointersPerBlock

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

func lookupByKeyHash(
	c *cache.Cache,
	rootPointer *pointerV0.Pointer,
	rootBlockType *blocks.BlockType,
	rootBlockSchemaVersion *blocks.SchemaVersion,
	create bool,
	keyHash uint64,
) (cache.Block[objectlistV0.Block], uint64, bool, func() (cache.Block[pointerV0.Block], error), error) {
	currentPointer := rootPointer
	currentBlockType := rootBlockType
	currentBlockSchemaVersion := rootBlockSchemaVersion
	var currentPointerBlock cache.Block[pointerV0.Block]
	var leafBlock cache.Block[objectlistV0.Block]

	hashReminder := keyHash

	var pointerIndex uint64
	for {
		switch *currentBlockType {
		case blocks.FreeBlockType:
			if create {
				if currentPointerBlock.IsValid() {
					currentPointerBlock.IncrementReferences()
					currentPointerBlock.Block.NUsedPointers++
				}

				var err error
				leafBlock, err = cache.NewBlock[objectlistV0.Block](c)
				if err != nil {
					return cache.Block[objectlistV0.Block]{}, 0, false, nil, err
				}
				leafBlock.WithPostCommitFunc(newLeafBlockPostCommitFunc(
					c,
					currentPointerBlock,
					currentPointer,
					currentBlockType,
					currentBlockSchemaVersion,
					leafBlock,
				))

				*currentPointer = pointerV0.Pointer{
					Address:       leafBlock.Address(),
					BirthRevision: c.SingularityBlock().Revision + 1,
				}
				*currentBlockType = blocks.LeafBlockType
				*currentBlockSchemaVersion = blocks.ObjectListV0

				return leafBlock, hashReminder, true, nil, nil
			}
			return cache.Block[objectlistV0.Block]{}, 0, false, nil, nil
		case blocks.LeafBlockType:
			// This is done to prevent pointer block from being unloaded when leaf block is added to the cache.
			if create && currentPointerBlock.IsValid() {
				currentPointerBlock.IncrementReferences()
			}

			var addedToCache bool
			var err error
			leafBlock, addedToCache, err = cache.FetchBlock[objectlistV0.Block](c, *currentPointer)
			if err != nil {
				return cache.Block[objectlistV0.Block]{}, 0, false, nil, err
			}

			if create {
				if addedToCache {
					leafBlock.WithPostCommitFunc(newLeafBlockPostCommitFunc(
						c,
						currentPointerBlock,
						currentPointer,
						currentBlockType,
						currentBlockSchemaVersion,
						leafBlock,
					))
				} else if currentPointerBlock.IsValid() {
					// If leaf block has been already present in the cache, it means that pointer block reference was incorrectly incremented,
					// and it must e fixed now.
					currentPointerBlock.DecrementReferences()
				}
			}

			splitFunc := func() (cache.Block[pointerV0.Block], error) {
				newPointerBlock, err := cache.NewBlock[pointerV0.Block](c)
				if err != nil {
					return cache.Block[pointerV0.Block]{}, err
				}

				newPointerBlock.WithPostCommitFunc(newPointerBlockPostCommitFunc(
					c,
					currentPointerBlock,
					currentPointer,
					currentBlockType,
					currentBlockSchemaVersion,
					newPointerBlock,
				))

				*currentPointer = pointerV0.Pointer{
					Address:       newPointerBlock.Address(),
					BirthRevision: newPointerBlock.BirthRevision(),
				}
				*currentBlockType = blocks.PointerBlockType
				*currentBlockSchemaVersion = blocks.PointerV0

				return newPointerBlock, nil
			}

			return leafBlock, hashReminder, true, splitFunc, nil
		case blocks.PointerBlockType:
			// This is done to prevent pointer block from being unloaded when leaf block is added to the cache.
			if create && currentPointerBlock.IsValid() {
				currentPointerBlock.IncrementReferences()
			}

			nextPointerBlock, addedToCache, err := cache.FetchBlock[pointerV0.Block](c, *currentPointer)
			if err != nil {
				return cache.Block[objectlistV0.Block]{}, 0, false, nil, err
			}

			if create {
				if addedToCache {
					nextPointerBlock.WithPostCommitFunc(newPointerBlockPostCommitFunc(
						c,
						currentPointerBlock,
						currentPointer,
						currentBlockType,
						currentBlockSchemaVersion,
						nextPointerBlock,
					))
				} else if currentPointerBlock.IsValid() {
					// If next pointer block has been already present in the cache, it means that previous pointer block reference was incorrectly incremented,
					// and it must e fixed now.
					currentPointerBlock.DecrementReferences()
				}
			}

			pointerIndex = hashReminder % pointerV0.PointersPerBlock
			hashReminder /= pointerV0.PointersPerBlock

			currentPointerBlock = nextPointerBlock
			currentPointer = &nextPointerBlock.Block.Pointers[pointerIndex]
			currentBlockType = &nextPointerBlock.Block.PointedBlockTypes[pointerIndex]
			currentBlockSchemaVersion = &nextPointerBlock.Block.PointedBlockVersions[pointerIndex]
		}
	}
}

func newLeafBlockPostCommitFunc(
	c *cache.Cache,
	parentBlock cache.Block[pointerV0.Block],
	parentPointer *pointerV0.Pointer,
	parentBlockType *blocks.BlockType,
	parentBlockSchemaVersion *blocks.SchemaVersion,
	leafBlock cache.Block[objectlistV0.Block],
) func() error {
	return func() error {
		*parentPointer = pointerV0.Pointer{
			Checksum:      blocks.BlockChecksum(leafBlock.Block),
			Address:       leafBlock.Address(),
			BirthRevision: leafBlock.BirthRevision(),
		}
		*parentBlockType = blocks.LeafBlockType
		*parentBlockSchemaVersion = blocks.ObjectListV0

		if parentBlock.IsValid() {
			parentBlock.DecrementReferences()
			if err := cache.DirtyBlock(c, parentBlock); err != nil {
				return err
			}
		}

		return nil
	}
}

func newPointerBlockPostCommitFunc(
	c *cache.Cache,
	parentBlock cache.Block[pointerV0.Block],
	parentPointer *pointerV0.Pointer,
	parentBlockType *blocks.BlockType,
	parentBlockSchemaVersion *blocks.SchemaVersion,
	pointerBlock cache.Block[pointerV0.Block],
) func() error {
	return func() error {
		*parentPointer = pointerV0.Pointer{
			Checksum:      blocks.BlockChecksum(pointerBlock.Block),
			Address:       pointerBlock.Address(),
			BirthRevision: pointerBlock.BirthRevision(),
		}
		*parentBlockType = blocks.PointerBlockType
		*parentBlockSchemaVersion = blocks.PointerV0

		if parentBlock.IsValid() {
			parentBlock.DecrementReferences()
			if err := cache.DirtyBlock(c, parentBlock); err != nil {
				return err
			}
		}

		return nil
	}
}
