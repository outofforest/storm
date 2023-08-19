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

// SingularityBlock returns modifiable singularity block.
func (c *Cache) SingularityBlock() *types.SingularityBlock {
	return c.singularityBlock.V
}

// FetchBlock retrieves block data from cache, if needed block is loaded from persistent source.
func (c *Cache) FetchBlock(address types.BlockAddress) ([]byte, error) {
	if address > c.singularityBlock.V.LastAllocatedBlock {
		return nil, errors.Errorf("block %d does not exist", address)
	}
	if address == 0 {
		return c.singularityBlock.B, nil
	}

	// TODO (wojciech): Implement open addressing
	// TODO (wojciech): Implement cache pruning if there is no free space
	// TODO (wojciech): Implement marking blocks in cache as deleted to prune them first if space is needed

	for offset := int64(0); offset < c.size; offset += CachedBlockSize {
		header := photon.NewFromBytes[Header](c.data[offset:])
		if header.V.State == FreeBlockState {
			if err := c.store.ReadBlock(address, c.data[offset+CacheHeaderSize:offset+CachedBlockSize]); err != nil {
				return nil, err
			}
			header.V.Address = address
			header.V.State = FetchedBlockState
			return c.data[offset+CacheHeaderSize : offset+CachedBlockSize], nil
		}
		if header.V.Address == address {
			return c.data[offset+CacheHeaderSize : offset+CachedBlockSize], nil
		}
	}

	return nil, errors.New("cache space exhausted")
}

// NewBlock allocates and returns new block.
func (c *Cache) NewBlock() (types.BlockAddress, []byte, error) {
	// TODO (wojciech): Implement open addressing
	// TODO (wojciech): Implement cache pruning if there is no free space
	// TODO (wojciech): Implement serious free space allocation

	for offset := int64(0); offset < c.size; offset += CachedBlockSize {
		header := photon.NewFromBytes[Header](c.data[offset:])
		if header.V.State == FreeBlockState {
			c.singularityBlock.V.LastAllocatedBlock++
			header.V.Address = c.singularityBlock.V.LastAllocatedBlock
			header.V.State = NewBlockState
			return c.singularityBlock.V.LastAllocatedBlock, c.data[offset+CacheHeaderSize : offset+CachedBlockSize], nil
		}
	}

	return 0, nil, errors.New("cache space exhausted")
}

// Sync synchronizes changes to the dev.
func (c *Cache) Sync() error {
	for offset := int64(0); offset < c.size; offset += CachedBlockSize {
		header := photon.NewFromBytes[Header](c.data[offset:])
		if header.V.State == FreeBlockState {
			break
		}

		// TODO (wojciech): Update checksums

		if header.V.State == NewBlockState {
			if err := c.store.WriteBlock(header.V.Address, c.data[offset+CacheHeaderSize:offset+CachedBlockSize]); err != nil {
				return err
			}
			header.V.State = FetchedBlockState
		}
	}

	if err := c.store.Sync(); err != nil {
		return err
	}

	// TODO (wojciech): Write many copies of the singularity block

	if err := c.store.WriteBlock(0, c.singularityBlock.B); err != nil {
		return err
	}

	if err := c.store.Sync(); err != nil {
		return err
	}

	// TODO (wojciech): Verify that singularity block is ok by rading it using O_DIRECT option

	return nil
}

// FetchBlock returns structure representing existing block of particular type.
func FetchBlock[T types.Block](cache *Cache, address types.BlockAddress) (T, error) {
	content, err := cache.FetchBlock(address)
	if err != nil {
		var b T
		return b, err
	}
	return *photon.NewFromBytes[T](content).V, nil
}

// NewBlock returns structure representing new block of particular type.
func NewBlock[T types.Block](cache *Cache, b T) (types.BlockAddress, error) {
	address, content, err := cache.NewBlock()
	if err != nil {
		return 0, err
	}
	block := photon.NewFromBytes[T](content)
	*block.V = b

	return address, nil
}
