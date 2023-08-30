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

	block := dataBlockPath.Leaf
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

	if dataBlockPath.Leaf.NUsedChunks >= objectlistV0.SplitTrigger {
		// TODO (wojciech): Check if split is possible - if all the keys have the same hash then it is not.
		pointerPointer, leafPointer, leafSchemaVersion, err := splitBlock(*dataBlockPath.Leaf, hashReminder, s.c)
		if err != nil {
			return 0, err
		}
		dataBlockPath, err = dataBlockPath.Split(pointerPointer, uint16(hashReminder%pointerV0.PointersPerBlock), leafPointer, leafSchemaVersion)
		if err != nil {
			return 0, err
		}

		hashReminder /= pointerV0.PointersPerBlock
	}

	objectID, created, err := ensureObjectID(sBlock, dataBlockPath.Leaf, key, hashReminder)
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

func splitBlock(block objectlistV0.Block, hashReminder uint64, c *cache.Cache) (pointerV0.Pointer, pointerV0.Pointer, blocks.SchemaVersion, error) {
	birthRevision := c.SingularityBlock().Revision + 1

	// Block representing the original hash reminder is created explicitly to be returned even if it's empty,
	// because later new key is inserted into it.
	returnedPointerIndex := uint16(hashReminder % pointerV0.PointersPerBlock)
	returnedBlock, returnedBlockAddress, err := cache.NewBlock[objectlistV0.Block](c)
	if err != nil {
		return pointerV0.Pointer{}, pointerV0.Pointer{}, 0, err
	}
	initObjectList(returnedBlock)
	returnedBlockChecksum := blocks.BlockChecksum(returnedBlock)

	pointerBlock, pointerBlockAddress, err := cache.NewBlock[pointerV0.Block](c)
	pointerBlock.NUsedPointers = 1
	pointerBlock.PointedBlockTypes[returnedPointerIndex] = blocks.LeafBlockType
	pointerBlock.PointedBlockVersions[returnedPointerIndex] = blocks.ObjectListV0
	pointerBlock.Pointers[returnedPointerIndex] = pointerV0.Pointer{
		Address:       returnedBlockAddress,
		Checksum:      returnedBlockChecksum,
		BirthRevision: birthRevision,
	}
	pointerBlockChecksum := blocks.BlockChecksum(pointerBlock)
	if err != nil {
		return pointerV0.Pointer{}, pointerV0.Pointer{}, 0, err
	}
	for i := uint16(0); i < objectlistV0.ChunksPerBlock; i++ {
		if block.ChunkPointerStates[i] != objectlistV0.DefinedChunkState {
			continue
		}
		pointerIndex := uint16(block.KeyHashReminders[i] % pointerV0.PointersPerBlock)

		var newBlock *objectlistV0.Block
		var newBlockAddress blocks.BlockAddress
		var err error
		var isNew bool
		if pointerBlock.PointedBlockTypes[pointerIndex] == blocks.FreeBlockType {
			isNew = true
			newBlock, newBlockAddress, err = cache.NewBlock[objectlistV0.Block](c)
			if err != nil {
				return pointerV0.Pointer{}, pointerV0.Pointer{}, 0, err
			}
			if newBlock.NUsedChunks == 0 {
				initObjectList(newBlock)
			}
		} else {
			newBlock, newBlockAddress, err = cache.CopyBlock[objectlistV0.Block](c, pointerBlock.Pointers[pointerIndex])
			if err != nil {
				return pointerV0.Pointer{}, pointerV0.Pointer{}, 0, err
			}
		}

		mergeChunkIntoBlock(newBlock, &block, i)
		newBlockChecksum := blocks.BlockChecksum(newBlock)

		if pointerIndex == returnedPointerIndex {
			returnedBlockAddress = newBlockAddress
			returnedBlockChecksum = newBlockChecksum
		}

		pointerBlock, pointerBlockAddress, err = cache.CopyBlock[pointerV0.Block](c, pointerV0.Pointer{
			Address:       pointerBlockAddress,
			Checksum:      pointerBlockChecksum,
			BirthRevision: birthRevision,
		})
		if err != nil {
			return pointerV0.Pointer{}, pointerV0.Pointer{}, 0, err
		}

		if isNew {
			pointerBlock.NUsedPointers++
		}
		pointerBlock.PointedBlockTypes[pointerIndex] = blocks.LeafBlockType
		pointerBlock.PointedBlockVersions[pointerIndex] = blocks.ObjectListV0
		pointerBlock.Pointers[pointerIndex] = pointerV0.Pointer{
			Address:       newBlockAddress,
			Checksum:      newBlockChecksum,
			BirthRevision: birthRevision,
		}
		pointerBlockChecksum = blocks.BlockChecksum(pointerBlock)
	}

	return pointerV0.Pointer{
			Address:       pointerBlockAddress,
			Checksum:      pointerBlockChecksum,
			BirthRevision: birthRevision,
		}, pointerV0.Pointer{
			Address:       returnedBlockAddress,
			Checksum:      returnedBlockChecksum,
			BirthRevision: birthRevision,
		}, blocks.ObjectListV0, nil
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
	Pointer      pointerV0.Pointer
	PointerIndex uint16
}

type keyPath[T blocks.Block] struct {
	c                      *cache.Cache
	rootPointer            *pointerV0.Pointer
	rootBlockType          *blocks.BlockType
	rootBlockSchemaVersion *blocks.SchemaVersion
	leafSchemaVersion      blocks.SchemaVersion
	leafAddress            blocks.BlockAddress
	hops                   []hop
	Leaf                   *T
}

func lookupByKeyHash[T blocks.Block](
	c *cache.Cache,
	rootPointer *pointerV0.Pointer,
	rootBlockType *blocks.BlockType,
	rootBlockSchemaVersion *blocks.SchemaVersion,
	leafSchemaVersion blocks.SchemaVersion,
	forUpdate bool,
	keyHash uint64,
) (keyPath[T], uint64, bool, error) {
	currentPointer := *rootPointer
	currentPointedBlockType := *rootBlockType

	hashReminder := keyHash
	hops := make([]hop, 0, 11) // TODO (wojciech): Check size

	for {
		switch currentPointedBlockType {
		case blocks.FreeBlockType:
			if forUpdate {
				leafBlock, address, err := cache.NewBlock[T](c)
				if err != nil {
					return keyPath[T]{}, 0, false, err
				}
				return keyPath[T]{
					c:                      c,
					rootPointer:            rootPointer,
					rootBlockType:          rootBlockType,
					rootBlockSchemaVersion: rootBlockSchemaVersion,
					leafAddress:            address,
					leafSchemaVersion:      leafSchemaVersion,
					hops:                   hops,
					Leaf:                   leafBlock,
				}, hashReminder, true, nil
			}
			return keyPath[T]{}, 0, false, nil
		case blocks.LeafBlockType:
			var leafBlock *T
			var leafAddress blocks.BlockAddress
			var err error
			if forUpdate {
				leafBlock, leafAddress, err = cache.CopyBlock[T](c, currentPointer)
			} else {
				leafBlock, leafAddress, err = cache.FetchBlock[T](c, currentPointer)
			}

			if err != nil {
				return keyPath[T]{}, 0, false, err
			}
			return keyPath[T]{
				c:                      c,
				rootPointer:            rootPointer,
				rootBlockType:          rootBlockType,
				rootBlockSchemaVersion: rootBlockSchemaVersion,
				leafAddress:            leafAddress,
				leafSchemaVersion:      leafSchemaVersion,
				hops:                   hops,
				Leaf:                   leafBlock,
			}, hashReminder, true, nil
		case blocks.PointerBlockType:
			pointerBlock, _, err := cache.FetchBlock[pointerV0.Block](c, currentPointer)
			if err != nil {
				return keyPath[T]{}, 0, false, err
			}

			pointerIndex := uint16(hashReminder % pointerV0.PointersPerBlock)
			hashReminder /= pointerV0.PointersPerBlock

			hops = append(hops, hop{
				Pointer:      currentPointer,
				PointerIndex: pointerIndex,
			})

			currentPointedBlockType = pointerBlock.PointedBlockTypes[pointerIndex]
			currentPointer = pointerBlock.Pointers[pointerIndex]
		}
	}
}

func (kp keyPath[T]) Split(
	pointerPointer pointerV0.Pointer,
	pointerIndex uint16,
	leafPointer pointerV0.Pointer,
	leafSchemaVersion blocks.SchemaVersion,
) (keyPath[T], error) {
	kp.hops = append(kp.hops, hop{
		Pointer:      pointerPointer,
		PointerIndex: pointerIndex,
	})

	leafBlock, leafAddress, err := cache.CopyBlock[T](kp.c, leafPointer)
	if err != nil {
		return keyPath[T]{}, err
	}

	kp.Leaf = leafBlock
	kp.leafAddress = leafAddress
	kp.leafSchemaVersion = leafSchemaVersion

	return kp, nil
}

func (kp keyPath[T]) Commit() (*T, error) {
	birthRevision := kp.c.SingularityBlock().Revision + 1

	pointer := pointerV0.Pointer{
		Address:       kp.leafAddress,
		Checksum:      blocks.BlockChecksum(kp.Leaf),
		BirthRevision: birthRevision,
	}

	if nHops := len(kp.hops); nHops > 0 {
		for i := nHops - 1; i >= 0; i-- {
			hopBlock, address, err := cache.CopyBlock[pointerV0.Block](kp.c, kp.hops[i].Pointer)
			if err != nil {
				return nil, err
			}
			hopIndex := kp.hops[i].PointerIndex

			// TODO (wojciech): Clean up this
			if i == nHops-1 {
				if hopBlock.PointedBlockTypes[hopIndex] == blocks.FreeBlockType {
					hopBlock.NUsedPointers++
					hopBlock.PointedBlockTypes[hopIndex] = blocks.LeafBlockType
					hopBlock.PointedBlockVersions[hopIndex] = kp.leafSchemaVersion
				}
			} else {
				hopBlock.PointedBlockTypes[hopIndex] = blocks.PointerBlockType
				hopBlock.PointedBlockVersions[hopIndex] = blocks.PointerV0
			}

			hopBlock.Pointers[hopIndex] = pointer

			pointer = pointerV0.Pointer{
				Address:       address,
				Checksum:      blocks.BlockChecksum(hopBlock),
				BirthRevision: birthRevision,
			}
		}

		*kp.rootBlockType = blocks.PointerBlockType
		*kp.rootBlockSchemaVersion = blocks.PointerV0
	} else if *kp.rootBlockType == blocks.FreeBlockType {
		*kp.rootBlockType = blocks.LeafBlockType
		*kp.rootBlockSchemaVersion = kp.leafSchemaVersion
	}
	*kp.rootPointer = pointer

	return kp.Leaf, nil
}
