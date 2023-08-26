package cache

import (
	"unsafe"

	"github.com/outofforest/photon"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/blocks"
	pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"
	singularityV0 "github.com/outofforest/storm/blocks/singularity/v0"
	"github.com/outofforest/storm/persistence"
)

var zeroContent = make([]byte, blocks.BlockSize)

// Cache caches blocks.
type Cache struct {
	store       *persistence.Store
	nBlocks     int64
	data        []byte
	dirtyBlocks map[int64]struct{}

	singularityBlock photon.Union[*singularityV0.Block]
}

// New creates new cache.
func New(store *persistence.Store, size int64) (*Cache, error) {
	sBlock := photon.NewFromValue(&singularityV0.Block{})
	if err := store.ReadBlock(0, sBlock.B); err != nil {
		return nil, err
	}

	return &Cache{
		store:            store,
		nBlocks:          size / CachedBlockSize,
		data:             make([]byte, size),
		dirtyBlocks:      make(map[int64]struct{}, MaxDirtyBlocks),
		singularityBlock: sBlock,
	}, nil
}

// SingularityBlock returns current singularity block.
func (c *Cache) SingularityBlock() *singularityV0.Block {
	return c.singularityBlock.V
}

// Commit commits changes to the device.
func (c *Cache) Commit() error {
	if err := c.commitData(); err != nil {
		return err
	}

	// TODO (wojciech): Write each new version to rotating location

	c.singularityBlock.V.Revision++
	c.singularityBlock.V.Checksum = c.singularityBlock.V.ComputeChecksum()
	if err := c.store.WriteBlock(0, c.singularityBlock.B); err != nil {
		return err
	}

	if err := c.store.Sync(); err != nil {
		return err
	}

	// TODO (wojciech): Verify that singularity block is ok by reading it using O_DIRECT option

	return nil
}

func (c *Cache) commitData() error {
	for cacheAddress := range c.dirtyBlocks {
		offset := cacheAddress * CachedBlockSize
		header := photon.NewFromBytes[header](c.data[offset:])
		if err := c.store.WriteBlock(header.V.Address, c.data[offset+CacheHeaderSize:offset+CachedBlockSize]); err != nil {
			return err
		}
		header.V.State = fetchedBlockState
	}

	// This is intentionally done in separate loop to take advantage of the optimisation
	// golang applies when seeing this code.
	for cacheAddress := range c.dirtyBlocks {
		delete(c.dirtyBlocks, cacheAddress)
	}

	return nil
}

func (c *Cache) fetchBlock(
	address blocks.BlockAddress,
	nBytes int64,
	expectedChecksum blocks.Hash,
) ([]byte, error) {
	if address > c.singularityBlock.V.LastAllocatedBlock {
		return nil, errors.Errorf("block %d does not exist", address)
	}

	// TODO (wojciech): Implement marking blocks in cache as deleted to prune them first if space is needed

	cacheAddress, err := c.findCachedBlock(address)
	if err != nil {
		return nil, err
	}

	offset := cacheAddress * CachedBlockSize
	dataOffset := offset + CacheHeaderSize
	blockBuffer := c.data[dataOffset : dataOffset+nBytes]

	h := photon.NewFromBytes[header](c.data[offset:])
	if h.V.State == fetchedBlockState || h.V.State == newBlockState {
		return c.data[offset : dataOffset+nBytes], nil
	}

	h.V.Address = address
	h.V.State = fetchedBlockState

	if err := c.store.ReadBlock(address, blockBuffer); err != nil {
		return nil, err
	}
	if err := blocks.VerifyChecksum(address, blockBuffer, expectedChecksum); err != nil {
		return nil, err
	}

	return c.data[offset : dataOffset+nBytes], nil
}

func (c *Cache) newBlock(nBytes int64) ([]byte, error) {
	address := c.singularityBlock.V.LastAllocatedBlock + 1
	cacheAddress, err := c.findCachedBlock(address)
	if err != nil {
		return nil, err
	}
	c.singularityBlock.V.LastAllocatedBlock++

	offset := cacheAddress * CachedBlockSize
	dataOffset := offset + CacheHeaderSize

	header := photon.NewFromBytes[header](c.data[offset:])
	header.V.Address = address
	header.V.State = newBlockState

	// This is done because memory used for padding in structs is not zeroed automatically,
	// causing mismatch in hashes.
	copy(c.data[dataOffset:dataOffset+nBytes], zeroContent)

	if len(c.dirtyBlocks) >= MaxDirtyBlocks {
		if err := c.commitData(); err != nil {
			return nil, err
		}
	}
	c.dirtyBlocks[cacheAddress] = struct{}{}

	return c.data[offset : dataOffset+nBytes], nil
}

func (c *Cache) findCachedBlock(address blocks.BlockAddress) (int64, error) {
	// TODO (wojciech): Implement cache pruning based on hit metric

	// For now, if there is no free space in cache found in `maxCacheTries` tries, the first tried block is replaced
	// by the fetched one.
	selectedCacheAddress := int64(address) % c.nBlocks
	var invalidCacheAddressFound bool

	// Multiplying by 3 instead of 2 here is better, because it produces both even and odd addresses.
	for i, cacheAddress := 0, selectedCacheAddress; i < MaxCacheTries; i, cacheAddress = i+1, (cacheAddress*3)%c.nBlocks {
		offset := cacheAddress * CachedBlockSize
		h := photon.NewFromBytes[header](c.data[offset:])

		switch h.V.State {
		case freeBlockState:
			if invalidCacheAddressFound {
				return selectedCacheAddress, nil
			}
			return cacheAddress, nil
		case invalidBlockState:
			if !invalidCacheAddressFound {
				invalidCacheAddressFound = true
				selectedCacheAddress = cacheAddress
			}
		case fetchedBlockState, newBlockState:
			if h.V.Address == address {
				return cacheAddress, nil
			}
		}
	}

	offset := selectedCacheAddress * CachedBlockSize
	dataOffset := offset + CacheHeaderSize

	h := photon.NewFromBytes[header](c.data[offset:])
	switch h.V.State {
	case newBlockState:
		if err := c.store.WriteBlock(h.V.Address, c.data[dataOffset:dataOffset+blocks.BlockSize]); err != nil {
			return 0, err
		}
		delete(c.dirtyBlocks, selectedCacheAddress)
		h.V.State = invalidBlockState
	case fetchedBlockState:
		h.V.State = invalidBlockState
	}

	return selectedCacheAddress, nil
}

// CachedBlock represents state of the block stored in cache.
type CachedBlock[T blocks.Block] struct {
	cache *Cache
	block photon.Union[*block[T]]
	Block T
}

// Address returns addrress of the block.
func (cb CachedBlock[T]) Address() (blocks.BlockAddress, error) {
	if cb.block.V == nil || cb.block.V.Header.State == freeBlockState {
		return 0, errors.New("block hasn't been allocated yet")
	}

	return cb.block.V.Header.Address, nil
}

// Commit commits block changes to cache.
func (cb CachedBlock[T]) Commit() (CachedBlock[T], error) {
	if cb.block.V == nil || cb.block.V.Header.State != newBlockState {
		var v T
		data, err := cb.cache.newBlock(int64(unsafe.Sizeof(v)))
		if err != nil {
			return CachedBlock[T]{}, err
		}
		cb.block = photon.NewFromBytes[block[T]](data)
	}

	cb.block.V.Block = cb.Block

	return cb, nil
}

// FetchBlock returns structure representing existing block of particular type.
func FetchBlock[T blocks.Block](
	cache *Cache,
	pointer pointerV0.Pointer,
) (CachedBlock[T], error) {
	var v T
	data, err := cache.fetchBlock(pointer.Address, int64(unsafe.Sizeof(v)), pointer.Checksum)
	if err != nil {
		return CachedBlock[T]{}, err
	}

	block := photon.NewFromBytes[block[T]](data)
	return CachedBlock[T]{
		cache: cache,
		block: block,
		Block: block.V.Block,
	}, nil
}

// NewBlock returns structure representing new block of particular type.
func NewBlock[T blocks.Block](cache *Cache) CachedBlock[T] {
	return CachedBlock[T]{
		cache: cache,
	}
}
