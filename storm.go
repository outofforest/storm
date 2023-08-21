package storm

import (
	"github.com/cespare/xxhash/v2"

	"github.com/outofforest/storm/cache"
	"github.com/outofforest/storm/persistence"
	"github.com/outofforest/storm/types"
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
func (s *Storm) Get(key [32]byte) ([32]byte, bool, error) {
	sBlock := s.c.SingularityBlock()
	dataBlockPath, exists, err := lookupByKey[types.DataBlock](s.c, &sBlock.RootData, &sBlock.RootDataBlockType, false, key)
	if !exists || err != nil {
		return [32]byte{}, false, err
	}

	dataBlock := dataBlockPath.Leaf.Block

	if dataBlock.RecordStates[0] == types.FreeRecordState {
		return [32]byte{}, false, nil
	}

	if hash := xxhash.Sum64(key[:]); dataBlock.RecordHashes[0] != hash {
		return [32]byte{}, false, nil
	}

	record := dataBlock.Records[0]

	if record.Key != key {
		return [32]byte{}, false, nil
	}

	return record.Value, true, nil
}

// Set sets value for a key in the store.
func (s *Storm) Set(key [32]byte, value [32]byte) error {
	// TODO (wojciech): Implement block splitting

	sBlock := s.c.SingularityBlock()
	dataBlockPath, _, err := lookupByKey[types.DataBlock](s.c, &sBlock.RootData, &sBlock.RootDataBlockType, true, key)
	if err != nil {
		return err
	}

	// TODO (wojciech): Find correct record index

	dataBlockPath.Leaf.Block.NUsedRecords = 1
	dataBlockPath.Leaf.Block.RecordStates[0] = types.DefinedRecordState
	dataBlockPath.Leaf.Block.RecordHashes[0] = xxhash.Sum64(key[:])
	dataBlockPath.Leaf.Block.Records[0] = types.Record{
		Key:   key,
		Value: value,
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

type hop struct {
	Index uint64
	Block cache.CachedBlock[types.PointerBlock]
}

type keyPath[T types.Block] struct {
	rootPointer   *types.Pointer
	rootBlockType *types.BlockType
	hops          []hop
	Leaf          cache.CachedBlock[T]
}

func lookupByKey[T types.Block](
	c *cache.Cache,
	rootPointer *types.Pointer,
	rootBlockType *types.BlockType,
	createIfMissing bool,
	key [32]byte,
) (keyPath[T], bool, error) {
	currentPointer := *rootPointer
	currentPointedBlockType := *rootBlockType

	hash := xxhash.Sum64(key[:])
	hopAddressing := hash
	hops := make([]hop, 0, 11)

	for {
		switch currentPointedBlockType {
		case types.FreeBlockType:
			if createIfMissing {
				return keyPath[T]{
					rootPointer:   rootPointer,
					rootBlockType: rootBlockType,
					hops:          hops,
					Leaf:          cache.NewBlock[T](c),
				}, true, nil
			}
			return keyPath[T]{}, false, nil
		case types.LeafBlockType:
			leafBlock, err := cache.FetchBlock[T](c, currentPointer.Address)
			if err != nil {
				return keyPath[T]{}, false, err
			}
			return keyPath[T]{
				rootPointer:   rootPointer,
				rootBlockType: rootBlockType,
				hops:          hops,
				Leaf:          leafBlock,
			}, true, nil
		case types.PointerBlockType:
			pointerBlock, err := cache.FetchBlock[types.PointerBlock](c, currentPointer.Address)
			if err != nil {
				return keyPath[T]{}, false, err
			}

			pointerIndex := hopAddressing & (types.PointersPerBlock - 1)
			hopAddressing >>= types.PointersPerBlockShift

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
	structCheksum, dataChecksum, err := leaf.Block.ComputeChecksums()
	if err != nil {
		return cache.CachedBlock[T]{}, err
	}

	if nHops := len(kp.hops); nHops > 0 {
		lastHop := kp.hops[nHops-1]
		if lastHop.Block.Block.PointedBlockTypes[lastHop.Index] == types.FreeBlockType {
			lastHop.Block.Block.NUsedPointers++
			lastHop.Block.Block.PointedBlockTypes[lastHop.Index] = types.LeafBlockType
		}
		for i := nHops - 1; i >= 0; i-- {
			hop := kp.hops[i]
			hop.Block.Block.Pointers[hop.Index] = types.Pointer{
				Address:        address,
				StructChecksum: structCheksum,
				DataChecksum:   dataChecksum,
			}
			pointerBlock, err := hop.Block.Commit()
			if err != nil {
				return cache.CachedBlock[T]{}, err
			}
			address, err = pointerBlock.Address()
			if err != nil {
				return cache.CachedBlock[T]{}, err
			}
			structCheksum, dataChecksum, err = leaf.Block.ComputeChecksums()
			if err != nil {
				return cache.CachedBlock[T]{}, err
			}
		}
	}

	if *kp.rootBlockType == types.FreeBlockType {
		*kp.rootBlockType = types.LeafBlockType
	}
	*kp.rootPointer = types.Pointer{
		StructChecksum: structCheksum,
		DataChecksum:   dataChecksum,
		Address:        address,
	}

	return leaf, nil
}
