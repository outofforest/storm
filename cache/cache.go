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

var zeroContent = make([]byte, CachedBlockSize)

// Cache caches blocks.
type Cache struct {
	store *persistence.Store
	size  int64
	data  []byte

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
		size:             size / CachedBlockSize * CachedBlockSize,
		data:             make([]byte, size),
		singularityBlock: sBlock,
	}, nil
}

// SingularityBlock returns current singularity block.
func (c *Cache) SingularityBlock() *singularityV0.Block {
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

func (c *Cache) fetchBlock(
	address blocks.BlockAddress,
	nBytes int64,
	expectedChecksum blocks.Hash,
) ([]byte, error) {
	if address > c.singularityBlock.V.LastAllocatedBlock {
		return nil, errors.Errorf("block %d does not exist", address)
	}

	// TODO (wojciech): Implement open addressing
	// TODO (wojciech): Implement cache pruning if there is no free space
	// TODO (wojciech): Implement marking blocks in cache as deleted to prune them first if space is needed

	for offset := int64(0); offset < c.size; offset += CachedBlockSize {
		h := photon.NewFromBytes[header](c.data[offset:])
		if h.V.State == freeBlockState {
			dataOffset := offset + CacheHeaderSize
			blockBuffer := c.data[dataOffset : dataOffset+nBytes]
			if err := c.store.ReadBlock(address, blockBuffer); err != nil {
				return nil, err
			}
			if err := blocks.VerifyChecksum(address, blockBuffer, expectedChecksum); err != nil {
				return nil, err
			}
			h.V.Address = address
			h.V.State = fetchedBlockState
			return c.data[offset : dataOffset+nBytes], nil
		}
		if h.V.Address == address {
			return c.data[offset : offset+CacheHeaderSize+nBytes], nil
		}
	}

	return nil, errors.New("cache space exhausted")
}

func (c *Cache) newBlock(nBytes int64) ([]byte, error) {
	// TODO (wojciech): Implement open addressing
	// TODO (wojciech): Implement cache pruning if there is no free space
	// TODO (wojciech): Implement serious free space allocation

	for offset := int64(0); offset < c.size; offset += CachedBlockSize {
		header := photon.NewFromBytes[header](c.data[offset:])
		if header.V.State == freeBlockState {
			dataBuffer := c.data[offset : offset+CacheHeaderSize+nBytes]

			// This is done because memory used for padding in structs is not zeroed automatically,
			// causing mismatch in hashes.
			copy(dataBuffer, zeroContent)

			c.singularityBlock.V.LastAllocatedBlock++
			header.V.Address = c.singularityBlock.V.LastAllocatedBlock
			header.V.State = newBlockState
			return dataBuffer, nil
		}
	}

	return nil, errors.New("cache space exhausted")
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
