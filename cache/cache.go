package cache

import (
	"github.com/outofforest/photon"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/persistence"
	"github.com/outofforest/storm/types"
)

const freeSlotAddress = 0

// Cache caches blocks.
type Cache struct {
	store *persistence.Store
	size  int64
	data  []byte

	singularityBlock CachedBlock[types.SingularityBlock]
}

// New creates new cache.
func New(store *persistence.Store, size int64) (*Cache, error) {
	sBlock := photon.NewFromValue(&types.SingularityBlock{})
	if err := store.ReadBlock(0, sBlock.B); err != nil {
		return nil, err
	}

	return &Cache{
		store: store,
		size:  size,
		data:  make([]byte, size),
		singularityBlock: CachedBlock[types.SingularityBlock]{
			Header: Header{
				Address: 0,
			},
			Block: sBlock,
		},
	}, nil
}

// Get retrieves block data from cache, if needed block is loaded from persistent source.
func (c *Cache) Get(address types.BlockAddress) (Header, []byte, error) {
	if address == 0 {
		return c.singularityBlock.Header, c.singularityBlock.Block.B, nil
	}

	// TODO (wojciech): Implement open addressing
	// TODO (wojciech): Implement cache pruning if there is no free space

	for offset := int64(0); offset < c.size; offset += CachedBlockSize {
		header := photon.NewFromBytes[Header](c.data[offset:])
		if header.V.Address == address {
			return *header.V, c.data[offset+CacheHeaderSize : offset+CachedBlockSize], nil
		}

		if header.V.Address == freeSlotAddress {
			header.V.Address = address
			if err := c.store.ReadBlock(address, c.data[offset+CacheHeaderSize:offset+CachedBlockSize]); err != nil {
				return Header{}, nil, err
			}
			return *header.V, c.data[offset+CacheHeaderSize : offset+CachedBlockSize], nil
		}
	}

	return Header{}, nil, errors.New("cache space exhausted")
}

// GetBlock returns structure representing the block of the particular type.
func GetBlock[T types.Block](cache *Cache, address types.BlockAddress) (CachedBlock[T], error) {
	header, content, err := cache.Get(address)
	if err != nil {
		return CachedBlock[T]{}, err
	}
	return CachedBlock[T]{
		Header: header,
		Block:  photon.NewFromBytes[T](content),
	}, nil
}
