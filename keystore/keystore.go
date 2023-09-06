package keystore

import (
	"bytes"
	"encoding/hex"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/objectlist"
	"github.com/outofforest/storm/blocks/pointer"
	"github.com/outofforest/storm/blocks/singularity"
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
	if len(key) > objectlist.MaxKeyComponentLength {
		return 0, false, errors.Errorf("maximum key component length exceeded, maximum: %d, actual: %d", objectlist.MaxKeyComponentLength, len(key))
	}

	sBlock := s.c.SingularityBlock()
	dataBlock, tagReminder, exists, err := cache.TraceTag[objectlist.Block](
		s.c,
		cache.BlockOrigin{
			Pointer:   &sBlock.RootData,
			BlockType: &sBlock.RootDataBlockType,
		},
		false,
		xxhash.Sum64(key),
	)
	if !exists || err != nil {
		return 0, false, err
	}

	index, chunkFound := findChunkPointerForKey(dataBlock.Block.Block, key, tagReminder)
	if chunkFound && dataBlock.Block.Block.ChunkPointerStates[index] == objectlist.DefinedChunkState {
		return dataBlock.Block.Block.ObjectLinks[index], true, nil
	}

	return 0, false, nil
}

// EnsureObjectID returns object ID for key. If the object ID does not exist it is created.
func (s *Store) EnsureObjectID(key []byte) (blocks.ObjectID, error) {
	if len(key) == 0 {
		return 0, errors.Errorf("key cannot be empty")
	}
	if len(key) > objectlist.MaxKeyComponentLength {
		return 0, errors.Errorf("maximum key component length exceeded, maximum: %d, actual: %d", objectlist.MaxKeyComponentLength, len(key))
	}

	sBlock := s.c.SingularityBlock()
	dataBlock, tagReminder, _, err := cache.TraceTag[objectlist.Block](
		s.c,
		cache.BlockOrigin{
			Pointer:   &sBlock.RootData,
			BlockType: &sBlock.RootDataBlockType,
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
	sBlock *singularity.Block,
	block cache.Trace[objectlist.Block],
	key []byte,
	tagReminder uint64,
) (blocks.ObjectID, error) {
	index, chunkFound := findChunkPointerForKey(block.Block.Block, key, tagReminder)
	if chunkFound && block.Block.Block.ChunkPointerStates[index] == objectlist.DefinedChunkState {
		for _, pointerBlock := range block.PointerBlocks {
			pointerBlock.DecrementReferences()
		}
		return block.Block.Block.ObjectLinks[index], nil
	}

	if block.Block.Block.NUsedChunks >= objectlist.SplitTrigger {
		// TODO (wojciech): Check if split is possible - if all the keys have the same hash then it is not.

		var err error
		block.Block, tagReminder, err = block.Split(func(newPointerBlock cache.Block[pointer.Block]) error {
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
	block *objectlist.Block,
	newPointerBlock cache.Block[pointer.Block],
) error {
	sBlock := s.c.SingularityBlock()
	for i := uint16(0); i < objectlist.ChunksPerBlock; i++ {
		if block.ChunkPointerStates[i] != objectlist.DefinedChunkState {
			continue
		}

		var newBlock cache.Block[objectlist.Block]
		pointerIndex := block.KeyTagReminders[i] % pointer.PointersPerBlock
		if newPointerBlock.Block.PointedBlockTypes[pointerIndex] == blocks.FreeBlockType {
			newPointerBlock.IncrementReferences()
			var err error
			newBlock, err = cache.NewBlock[objectlist.Block](s.c)
			if err != nil {
				return err
			}

			newBlock.WithPostCommitFunc(cache.NewLeafBlockPostCommitFunc(
				s.c,
				cache.BlockOrigin{
					PointerBlock: newPointerBlock,
					Pointer:      &newPointerBlock.Block.Pointers[pointerIndex],
					BlockType:    &newPointerBlock.Block.PointedBlockTypes[pointerIndex],
				},
				newBlock,
			))

			newPointerBlock.Block.Pointers[pointerIndex] = pointer.Pointer{
				Address:       newBlock.Address(),
				BirthRevision: sBlock.Revision + 1,
			}
			newPointerBlock.Block.PointedBlockTypes[pointerIndex] = blocks.LeafBlockType

			initFreeChunkList(newBlock.Block)
		} else {
			newPointerBlock.IncrementReferences()

			var addedToCache bool
			var err error
			newBlock, addedToCache, err = cache.FetchBlock[objectlist.Block](s.c, newPointerBlock.Block.Pointers[pointerIndex])
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
						PointerBlock: newPointerBlock,
						Pointer:      &newPointerBlock.Block.Pointers[pointerIndex],
						BlockType:    &newPointerBlock.Block.PointedBlockTypes[pointerIndex],
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

func verifyKeyInChunks(key []byte, tagReminder uint64, block *objectlist.Block, index uint16) bool {
	if block.KeyTagReminders[index] != tagReminder {
		return false
	}

	chunkIndex := block.ChunkPointers[index]
	for {
		chunkOffset := uint64(chunkIndex) * objectlist.ChunkSize
		nextChunkIndex := block.NextChunkPointers[chunkIndex]
		if nextChunkIndex > objectlist.ChunksPerBlock {
			// This is the last chunk in the sequence.
			remainingLength := uint64(nextChunkIndex) - objectlist.ChunksPerBlock
			return bytes.Equal(key, block.Blob[chunkOffset:chunkOffset+remainingLength])
		}

		if !bytes.Equal(key[:objectlist.ChunkSize], block.Blob[chunkOffset:chunkOffset+objectlist.ChunkSize]) {
			return false
		}
		key = key[objectlist.ChunkSize:]
		chunkIndex = nextChunkIndex
	}
}

func setKeyInChunks(
	block *objectlist.Block,
	index uint16,
	key []byte,
	tagReminder uint64,
) error {
	nBlocksRequired := (uint16(len(key)) + objectlist.ChunkSize - 1) / objectlist.ChunkSize
	if block.NUsedChunks+nBlocksRequired > objectlist.ChunksPerBlock {
		// At this point, if block hasn't been split before, it means that all the chunks
		// are taken by keys producing the same hash, meaning that it's not possible to set the key.
		return errors.New("block does not contain enough free chunks to add new key")
	}

	if block.NUsedChunks == 0 {
		initFreeChunkList(block)
	}

	block.ChunkPointerStates[index] = objectlist.DefinedChunkState
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

		chunkOffset := uint64(lastChunkIndex) * objectlist.ChunkSize

		nCopied = copy(block.Blob[chunkOffset:chunkOffset+objectlist.ChunkSize], keyToCopy)
		keyToCopy = keyToCopy[nCopied:]
	}

	// If pointer is higher than `ChunksPerBlock` it means that this is the last chunk in the sequence
	// and value (index - ChunksPerBlock) indicates the remaining length of the key to read from the last chunk.
	block.NextChunkPointers[lastChunkIndex] = uint16(objectlist.ChunksPerBlock + nCopied)

	return nil
}

func findChunkPointerForKey(
	block *objectlist.Block,
	key []byte,
	tagReminder uint64,
) (uint16, bool) {
	var invalidChunkFound bool
	var invalidChunkIndex uint16

	chunkOffsetSeed := uint16(tagReminder % objectlist.ChunksPerBlock)
	for i := 0; i < objectlist.ChunksPerBlock; i++ {
		index := (chunkOffsetSeed + objectlist.AddressingOffsets[i]) % objectlist.ChunksPerBlock
		switch block.ChunkPointerStates[index] {
		case objectlist.DefinedChunkState:
			if key == nil || !verifyKeyInChunks(key, tagReminder, block, index) {
				continue
			}
		case objectlist.InvalidChunkState:
			if !invalidChunkFound {
				invalidChunkFound = true
				invalidChunkIndex = index
			}
			continue
		case objectlist.FreeChunkState:
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

func initFreeChunkList(block *objectlist.Block) {
	for i := uint16(0); i < objectlist.ChunksPerBlock; i++ {
		block.NextChunkPointers[i] = i + 1
	}
}

func copyKeyChunksBetweenBlocks(dstBlock *objectlist.Block, srcBlock *objectlist.Block, srcIndex uint16) {
	objectID := srcBlock.ObjectLinks[srcIndex]

	dstTagReminder := srcBlock.KeyTagReminders[srcIndex] / pointer.PointersPerBlock

	dstIndex, _ := findChunkPointerForKey(dstBlock, nil, dstTagReminder)
	dstBlock.ChunkPointerStates[dstIndex] = objectlist.DefinedChunkState
	dstBlock.ChunkPointers[dstIndex] = dstBlock.FreeChunkIndex
	dstBlock.ObjectLinks[dstIndex] = objectID
	dstBlock.KeyTagReminders[dstIndex] = dstTagReminder

	srcChunkIndex := srcBlock.ChunkPointers[srcIndex]
	var dstLastChunkIndex uint16
	for srcChunkIndex < objectlist.ChunksPerBlock {
		dstBlock.NUsedChunks++
		dstLastChunkIndex = dstBlock.FreeChunkIndex
		dstBlock.FreeChunkIndex = dstBlock.NextChunkPointers[dstBlock.FreeChunkIndex]
		dstBlock.NextChunkPointers[dstLastChunkIndex] = dstBlock.FreeChunkIndex

		srcChunkOffset := uint64(srcChunkIndex) * objectlist.ChunkSize
		dstChunkOffset := uint64(dstLastChunkIndex) * objectlist.ChunkSize

		copy(dstBlock.Blob[dstChunkOffset:dstChunkOffset+objectlist.ChunkSize], srcBlock.Blob[srcChunkOffset:srcChunkOffset+objectlist.ChunkSize])
		srcChunkIndex = srcBlock.NextChunkPointers[srcChunkIndex]
	}
	// For the last record `srcChunkIndex` contains `ChunkSize+remaininglength` so it might be assigned directly.
	dstBlock.NextChunkPointers[dstLastChunkIndex] = srcChunkIndex
}
