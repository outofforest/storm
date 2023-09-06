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
	dataBlock, tagReminder, exists, err := cache.TraceTag[objectlistV0.Block](
		s.c,
		cache.BlockOrigin{
			Pointer:            &sBlock.RootData,
			BlockType:          &sBlock.RootDataBlockType,
			BlockSchemaVersion: &sBlock.RootDataSchemaVersion,
		},
		false,
		xxhash.Sum64(key),
	)
	if !exists || err != nil {
		return 0, false, err
	}

	index, chunkFound := findChunkPointerForKey(dataBlock.Block.Block, key, tagReminder)
	if chunkFound && dataBlock.Block.Block.ChunkPointerStates[index] == objectlistV0.DefinedChunkState {
		return dataBlock.Block.Block.ObjectLinks[index], true, nil
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
	dataBlock, tagReminder, _, err := cache.TraceTag[objectlistV0.Block](
		s.c,
		cache.BlockOrigin{
			Pointer:            &sBlock.RootData,
			BlockType:          &sBlock.RootDataBlockType,
			BlockSchemaVersion: &sBlock.RootDataSchemaVersion,
		},
		true,
		xxhash.Sum64(key),
	)
	if err != nil {
		return 0, err
	}

	return s.ensureObjectID(sBlock, dataBlock, key, tagReminder)
}

// Delete deletes key from the store.
func (s *Store) Delete(key [32]byte) error {
	// TODO (wojciech): To be implemented
	return nil
}

func (s *Store) ensureObjectID(
	sBlock *singularityV0.Block,
	block cache.Trace[objectlistV0.Block],
	key []byte,
	tagReminder uint64,
) (blocks.ObjectID, error) {
	index, chunkFound := findChunkPointerForKey(block.Block.Block, key, tagReminder)
	if chunkFound && block.Block.Block.ChunkPointerStates[index] == objectlistV0.DefinedChunkState {
		for _, pointerBlock := range block.PointerBlocks {
			pointerBlock.DecrementReferences()
		}
		return block.Block.Block.ObjectLinks[index], nil
	}

	if block.Block.Block.NUsedChunks >= objectlistV0.SplitTrigger {
		// TODO (wojciech): Check if split is possible - if all the keys have the same hash then it is not.

		var err error
		block.Block, tagReminder, err = block.Split(func(newPointerBlock cache.Block[pointerV0.Block]) error {
			return s.splitBlock(block.Block.Block, newPointerBlock)
		})
		if err != nil {
			return 0, err
		}

		if block.Block.Block.NUsedChunks == 0 {
			initFreeChunkList(block.Block.Block)
		}

		index, chunkFound = findChunkPointerForKey(block.Block.Block, key, tagReminder)
	}

	if !chunkFound {
		return 0, errors.Errorf("cannot find chunk for key %s", hex.EncodeToString(key))
	}

	if err := setKeyInChunks(block.Block.Block, index, key, tagReminder); err != nil {
		return 0, err
	}

	block.Block.Block.ObjectLinks[index] = sBlock.NextObjectID
	if err := cache.DirtyBlock(s.c, block.Block); err != nil {
		return 0, err
	}

	sBlock.NextObjectID++

	return block.Block.Block.ObjectLinks[index], nil
}

func (s *Store) splitBlock(
	block *objectlistV0.Block,
	newPointerBlock cache.Block[pointerV0.Block],
) error {
	sBlock := s.c.SingularityBlock()
	for i := uint16(0); i < objectlistV0.ChunksPerBlock; i++ {
		if block.ChunkPointerStates[i] != objectlistV0.DefinedChunkState {
			continue
		}

		var newBlock cache.Block[objectlistV0.Block]
		pointerIndex := block.KeyTagReminders[i] % pointerV0.PointersPerBlock
		if newPointerBlock.Block.PointedBlockTypes[pointerIndex] == blocks.FreeBlockType {
			newPointerBlock.IncrementReferences()
			var err error
			newBlock, err = cache.NewBlock[objectlistV0.Block](s.c)
			if err != nil {
				return err
			}

			newBlock.WithPostCommitFunc(cache.NewLeafBlockPostCommitFunc(
				s.c,
				cache.BlockOrigin{
					PointerBlock:       newPointerBlock,
					Pointer:            &newPointerBlock.Block.Pointers[pointerIndex],
					BlockType:          &newPointerBlock.Block.PointedBlockTypes[pointerIndex],
					BlockSchemaVersion: &newPointerBlock.Block.PointedBlockVersions[pointerIndex],
				},
				newBlock,
			))

			newPointerBlock.Block.Pointers[pointerIndex] = pointerV0.Pointer{
				Address:       newBlock.Address(),
				BirthRevision: sBlock.Revision + 1,
			}
			newPointerBlock.Block.PointedBlockTypes[pointerIndex] = blocks.LeafBlockType
			newPointerBlock.Block.PointedBlockVersions[pointerIndex] = blocks.ObjectListV0

			initFreeChunkList(newBlock.Block)
		} else {
			newPointerBlock.IncrementReferences()

			var addedToCache bool
			var err error
			newBlock, addedToCache, err = cache.FetchBlock[objectlistV0.Block](s.c, newPointerBlock.Block.Pointers[pointerIndex])
			if err != nil {
				return err
			}

			if newBlock.Block.NUsedChunks == 0 {
				initFreeChunkList(newBlock.Block)
			}

			if addedToCache {
				newBlock.WithPostCommitFunc(cache.NewLeafBlockPostCommitFunc(
					s.c,
					cache.BlockOrigin{
						PointerBlock:       newPointerBlock,
						Pointer:            &newPointerBlock.Block.Pointers[pointerIndex],
						BlockType:          &newPointerBlock.Block.PointedBlockTypes[pointerIndex],
						BlockSchemaVersion: &newPointerBlock.Block.PointedBlockVersions[pointerIndex],
					},
					newBlock,
				))
			} else {
				newPointerBlock.DecrementReferences()
			}
		}

		copyKeyChunksBetweenBlocks(newBlock.Block, block, i)
		if err := cache.DirtyBlock(s.c, newBlock); err != nil {
			return err
		}
	}

	return nil
}

func verifyKeyInChunks(key []byte, tagReminder uint64, block *objectlistV0.Block, index uint16) bool {
	if block.KeyTagReminders[index] != tagReminder {
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

func setKeyInChunks(
	block *objectlistV0.Block,
	index uint16,
	key []byte,
	tagReminder uint64,
) error {
	nBlocksRequired := (uint16(len(key)) + objectlistV0.ChunkSize - 1) / objectlistV0.ChunkSize
	if block.NUsedChunks+nBlocksRequired > objectlistV0.ChunksPerBlock {
		// At this point, if block hasn't been split before, it means that all the chunks
		// are taken by keys producing the same hash, meaning that it's not possible to set the key.
		return errors.New("block does not contain enough free chunks to add new key")
	}

	if block.NUsedChunks == 0 {
		initFreeChunkList(block)
	}

	block.ChunkPointerStates[index] = objectlistV0.DefinedChunkState
	block.ChunkPointers[index] = block.FreeChunkIndex
	block.KeyTagReminders[index] = tagReminder

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

func findChunkPointerForKey(
	block *objectlistV0.Block,
	key []byte,
	tagReminder uint64,
) (uint16, bool) {
	var invalidChunkFound bool
	var invalidChunkIndex uint16

	chunkOffsetSeed := uint16(tagReminder % objectlistV0.ChunksPerBlock)
	for i := 0; i < objectlistV0.ChunksPerBlock; i++ {
		index := (chunkOffsetSeed + objectlistV0.AddressingOffsets[i]) % objectlistV0.ChunksPerBlock
		switch block.ChunkPointerStates[index] {
		case objectlistV0.DefinedChunkState:
			if key == nil || !verifyKeyInChunks(key, tagReminder, block, index) {
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

func initFreeChunkList(block *objectlistV0.Block) {
	for i := uint16(0); i < objectlistV0.ChunksPerBlock; i++ {
		block.NextChunkPointers[i] = i + 1
	}
}

func copyKeyChunksBetweenBlocks(dstBlock *objectlistV0.Block, srcBlock *objectlistV0.Block, srcIndex uint16) {
	objectID := srcBlock.ObjectLinks[srcIndex]

	dstTagReminder := srcBlock.KeyTagReminders[srcIndex] / pointerV0.PointersPerBlock

	dstIndex, _ := findChunkPointerForKey(dstBlock, nil, dstTagReminder)
	dstBlock.ChunkPointerStates[dstIndex] = objectlistV0.DefinedChunkState
	dstBlock.ChunkPointers[dstIndex] = dstBlock.FreeChunkIndex
	dstBlock.ObjectLinks[dstIndex] = objectID
	dstBlock.KeyTagReminders[dstIndex] = dstTagReminder

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
