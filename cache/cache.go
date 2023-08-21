package cache

import (
	"github.com/outofforest/photon"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/persistence"
	"github.com/outofforest/storm/types"
)

// Cache caches blocks.
type Cache struct {
	store *persistence.Store
	size  int64
	data  []byte

	singularityBlock photon.Union[types.SingularityBlock]
}

// New creates new cache.
func New(store *persistence.Store, size int64) (*Cache, error) {
	sBlock := photon.NewFromValue(&types.SingularityBlock{})
	if err := store.ReadBlock(0, sBlock.B); err != nil {
		return nil, err
	}

	return &Cache{
		store:            store,
		size:             size,
		data:             make([]byte, size),
		singularityBlock: sBlock,
	}, nil
}

// SingularityBlock returns current singularity block.
func (c *Cache) SingularityBlock() *types.SingularityBlock {
	return c.singularityBlock.V
}

// Commit commits changes to the device.
func (c *Cache) Commit() error {
	for offset := int64(0); offset < c.size; offset += CachedBlockSize {
		header := photon.NewFromBytes[header](c.data[offset:])
		if header.V.State == freeBlockState {
			break
		}

		// TODO (wojciech): Update checksums

		if header.V.State == newBlockState {
			if err := c.store.WriteBlock(header.V.Address, c.data[offset+CacheHeaderSize:offset+CachedBlockSize]); err != nil {
				return err
			}
			header.V.State = fetchedBlockState
		}
	}

	if err := c.store.Sync(); err != nil {
		return err
	}

	// TODO (wojciech): Write each new version to rotating location

	c.singularityBlock.V.Revision++
	checksum, _, err := c.singularityBlock.V.ComputeChecksums()
	if err != nil {
		return err
	}
	c.singularityBlock.V.StructChecksum = checksum
	if err := c.store.WriteBlock(0, c.singularityBlock.B); err != nil {
		return err
	}

	if err := c.store.Sync(); err != nil {
		return err
	}

	// TODO (wojciech): Verify that singularity block is ok by reading it using O_DIRECT option

	return nil
}

func (c *Cache) fetchBlock(address types.BlockAddress) (header, []byte, error) {
	if address > c.singularityBlock.V.LastAllocatedBlock {
		return header{}, nil, errors.Errorf("block %d does not exist", address)
	}
	if address == 0 {
		return header{}, c.singularityBlock.B, nil
	}

	// TODO (wojciech): Implement open addressing
	// TODO (wojciech): Implement cache pruning if there is no free space
	// TODO (wojciech): Implement marking blocks in cache as deleted to prune them first if space is needed

	for offset := int64(0); offset < c.size; offset += CachedBlockSize {
		h := photon.NewFromBytes[header](c.data[offset:])
		if h.V.State == freeBlockState {
			if err := c.store.ReadBlock(address, c.data[offset+CacheHeaderSize:offset+CachedBlockSize]); err != nil {
				return header{}, nil, err
			}
			h.V.Address = address
			h.V.State = fetchedBlockState
			return *h.V, c.data[offset+CacheHeaderSize : offset+CachedBlockSize], nil
		}
		if h.V.Address == address {
			return *h.V, c.data[offset+CacheHeaderSize : offset+CachedBlockSize], nil
		}
	}

	return header{}, nil, errors.New("cache space exhausted")
}

func (c *Cache) newBlock() (header, []byte, error) {
	// TODO (wojciech): Implement open addressing
	// TODO (wojciech): Implement cache pruning if there is no free space
	// TODO (wojciech): Implement serious free space allocation

	for offset := int64(0); offset < c.size; offset += CachedBlockSize {
		header := photon.NewFromBytes[header](c.data[offset:])
		if header.V.State == freeBlockState {
			c.singularityBlock.V.LastAllocatedBlock++
			header.V.Address = c.singularityBlock.V.LastAllocatedBlock
			header.V.State = newBlockState
			return *header.V, c.data[offset+CacheHeaderSize : offset+CachedBlockSize], nil
		}
	}

	return header{}, nil, errors.New("cache space exhausted")
}

// CachedBlock represents state of the block stored in cache.
type CachedBlock[T types.Block] struct {
	cache  *Cache
	header header
	block  photon.Union[T]
	Block  T
}

// Address returns addrress of the block.
func (cb CachedBlock[T]) Address() (types.BlockAddress, error) {
	if cb.header.State == freeBlockState {
		return 0, errors.New("block hasn't been allocated yet")
	}

	return cb.header.Address, nil
}

// Commit commits block changes to cache.
func (cb CachedBlock[T]) Commit() (CachedBlock[T], error) {
	if cb.header.State != newBlockState {
		header, data, err := cb.cache.newBlock()
		if err != nil {
			return CachedBlock[T]{}, err
		}
		cb.header = header
		cb.block = photon.NewFromBytes[T](data)
	}

	*cb.block.V = cb.Block

	return cb, nil
}

// FetchBlock returns structure representing existing block of particular type.
func FetchBlock[T types.Block](cache *Cache, address types.BlockAddress) (CachedBlock[T], error) {
	header, data, err := cache.fetchBlock(address)
	if err != nil {
		return CachedBlock[T]{}, err
	}

	block := photon.NewFromBytes[T](data)
	return CachedBlock[T]{
		cache:  cache,
		header: header,
		block:  block,
		Block:  *block.V,
	}, nil
}

// NewBlock returns structure representing new block of particular type.
func NewBlock[T types.Block](cache *Cache) CachedBlock[T] {
	return CachedBlock[T]{
		cache: cache,
	}
}
