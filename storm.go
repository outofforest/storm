package storm

import (
	"github.com/cespare/xxhash/v2"

	"github.com/outofforest/storm/cache"
	"github.com/outofforest/storm/persistence"
	"github.com/outofforest/storm/types"
)

// Storm represents the storm storage engine.
type Storm struct {
	cache *cache.Cache
}

// New returns new storm store.
func New(dev persistence.Dev, cacheSize int64) (*Storm, error) {
	store, err := persistence.OpenStore(dev)
	if err != nil {
		return nil, err
	}
	cache, err := cache.New(store, cacheSize)
	if err != nil {
		return nil, err
	}
	return &Storm{
		cache: cache,
	}, nil
}

// Get gets value for a key from the store.
func (s *Storm) Get(key [32]byte) (uint64, bool, error) {
	nextAddress := s.cache.SingularityBlock().RootData.Address

	hash := xxhash.Sum64(key[:])
	hopAddressing := hash

	var dataBlock cache.CachedBlock[types.DataBlock]
	var pointerBlock cache.CachedBlock[types.PointerBlock]
	var pointerIndex uint64

dataBlockLoop:
	for {
		var err error
		pointerBlock, err = cache.FetchBlock[types.PointerBlock](s.cache, nextAddress)
		if err != nil {
			return 0, false, err
		}

		pointerIndex = hash & (types.PointersPerBlock - 1)
		hopAddressing >>= types.PointersPerBlockShift

		switch pointerBlock.Block.PointedBlockTypes[pointerIndex] {
		case types.FreeBlockType:
			return 0, false, nil
		case types.DataBlockType:
			var err error
			dataBlock, err = cache.FetchBlock[types.DataBlock](s.cache, pointerBlock.Block.Pointers[pointerIndex].Address)
			if err != nil {
				return 0, false, err
			}
			break dataBlockLoop
		case types.PointerBlockType:
			nextAddress = pointerBlock.Block.Pointers[pointerIndex].Address
		}
	}

	if dataBlock.Block.RecordStates[0] == types.FreeRecordState {
		return 0, false, nil
	}

	if dataBlock.Block.RecordHashes[0] != hash {
		return 0, false, nil
	}

	record := dataBlock.Block.Records[0]

	if record.Key != key {
		return 0, false, nil
	}

	return record.Value, true, nil
}

type hop struct {
	Block cache.CachedBlock[types.PointerBlock]
	Index uint64
}

// Set sets value for a key in the store.
func (s *Storm) Set(key [32]byte, value uint64) error {
	sBlock := s.cache.SingularityBlock()
	nextAddress := sBlock.RootData.Address

	hash := xxhash.Sum64(key[:])
	hopAddressing := hash

	var dataBlock cache.CachedBlock[types.DataBlock]
	var pointerBlock cache.CachedBlock[types.PointerBlock]
	var pointerIndex uint64
	hops := make([]hop, 0, 11)

dataBlockLoop:
	for {
		var err error
		pointerBlock, err = cache.FetchBlock[types.PointerBlock](s.cache, nextAddress)
		if err != nil {
			return err
		}

		pointerIndex = hash & (types.PointersPerBlock - 1)
		hopAddressing >>= types.PointersPerBlockShift

		switch pointerBlock.Block.PointedBlockTypes[pointerIndex] {
		case types.FreeBlockType:
			dataBlock = cache.NewBlock[types.DataBlock](s.cache)
			pointerBlock.Block.NUsedPointers++
			pointerBlock.Block.PointedBlockTypes[pointerIndex] = types.DataBlockType
			break dataBlockLoop
		case types.DataBlockType:
			var err error
			dataBlock, err = cache.FetchBlock[types.DataBlock](s.cache, pointerBlock.Block.Pointers[pointerIndex].Address)
			if err != nil {
				return err
			}
			break dataBlockLoop
		case types.PointerBlockType:
			hops = append(hops, hop{
				Block: pointerBlock,
				Index: pointerIndex,
			})
			nextAddress = pointerBlock.Block.Pointers[pointerIndex].Address
		}
	}

	// TODO (wojciech): Find correct record index

	dataBlock.Block.NUsedRecords = 1
	dataBlock.Block.RecordStates[0] = types.DefinedRecordState
	dataBlock.Block.RecordHashes[0] = hash
	dataBlock.Block.Records[0] = types.Record{
		Key:   key,
		Value: value,
	}

	var err error
	dataBlock, err = dataBlock.Commit()
	if err != nil {
		return err
	}
	dataBlockAddress, err := dataBlock.Address()
	if err != nil {
		return err
	}

	pointerBlock.Block.Pointers[pointerIndex] = types.Pointer{
		Address: dataBlockAddress,
		// TODO (wojciech): Set checksums
	}
	pointerBlock, err = pointerBlock.Commit()
	if err != nil {
		return err
	}
	address, err := pointerBlock.Address()
	if err != nil {
		return err
	}

	for i := len(hops) - 1; i >= 0; i-- {
		hop := hops[i]
		hop.Block.Block.Pointers[hop.Index] = types.Pointer{
			Address: address,
			// TODO (wojciech): Set checksums
		}
		pointerBlock, err = hop.Block.Commit()
		if err != nil {
			return err
		}
		address, err = pointerBlock.Address()
		if err != nil {
			return err
		}
	}

	sBlock.RootData.Address = address
	// TODO (wojciech): Set checksums

	return nil
}

// Delete deletes key from the store.
func (s *Storm) Delete(key [32]byte) error {
	// TODO (wojciech): To be implemented
	return nil
}

// Commit commits cached changes to the device.
func (s *Storm) Commit() error {
	return s.cache.Commit()
}
