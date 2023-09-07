package cache

import (
	"math/rand"
	"unsafe"

	"github.com/outofforest/photon"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/blocks/pointer"
	"github.com/outofforest/storm/blocks/singularity"
	"github.com/outofforest/storm/persistence"
)

var zeroContent = make([]byte, blocks.BlockSize)

// Cache caches blocks.
type Cache struct {
	store             *persistence.Store
	nBlocks           uint64
	data              []byte
	blocks            []metadata
	addressingOffsets []uint64
	dirtyBlocks       map[*metadata]struct{} // TODO (wojciech): Limit the number of dirty blocks
	singularityBlock  photon.Union[*singularity.Block]
}

// New creates new cache.
func New(store *persistence.Store, size int64) (*Cache, error) {
	sBlock := photon.NewFromValue(&singularity.Block{})
	if err := store.ReadBlock(0, sBlock.B); err != nil {
		return nil, err
	}

	nBlocks := uint64(size) / uint64(blocks.BlockSize)
	data := make([]byte, nBlocks*uint64(blocks.BlockSize))
	bs := make([]metadata, nBlocks)
	for i, offset := 0, int64(0); i < len(bs); i, offset = i+1, offset+blocks.BlockSize {
		bs[i].Data = data[offset : offset+blocks.BlockSize]
	}

	addressingOffsets := make([]uint64, nBlocks)
	for i, v := range rand.New(rand.NewSource(0)).Perm(int(nBlocks)) {
		addressingOffsets[i] = uint64(v)
	}

	return &Cache{
		store:             store,
		nBlocks:           nBlocks,
		data:              data,
		blocks:            bs,
		addressingOffsets: addressingOffsets,
		dirtyBlocks:       map[*metadata]struct{}{},
		singularityBlock:  sBlock,
	}, nil
}

// SingularityBlock returns current singularity block.
func (c *Cache) SingularityBlock() *singularity.Block {
	return c.singularityBlock.V
}

// Commit commits changes to the device.
func (c *Cache) Commit() error {
	if err := c.commitData(); err != nil {
		return err
	}

	// TODO (wojciech): Write each new version to rotating location

	c.singularityBlock.V.Revision++
	c.singularityBlock.V.Checksum = 0
	c.singularityBlock.V.Checksum = blocks.BlockChecksum(c.singularityBlock.V)
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
	for len(c.dirtyBlocks) > 0 {
		for meta := range c.dirtyBlocks {
			if meta.NReferences > 0 {
				continue
			}

			addrBefore := meta.Address
			if err := c.commitBlock(meta); err != nil {
				return err
			}
			if meta.Address != addrBefore {
				meta.State = invalidBlockState
				meta2, err := c.findCachedBlock(meta.Address, meta.BirthRevision)
				if err != nil {
					return err
				}

				meta2.State = usedBlockState
				meta2.Data, meta.Data = meta.Data, meta2.Data
			}
		}
	}
	return nil
}

func (c *Cache) commitBlock(meta *metadata) error {
	if meta.BirthRevision <= c.singularityBlock.V.Revision {
		c.singularityBlock.V.LastAllocatedBlock++
		meta.Address = c.singularityBlock.V.LastAllocatedBlock
		meta.BirthRevision = c.singularityBlock.V.Revision + 1
	}
	if err := c.store.WriteBlock(meta.Address, meta.Data); err != nil {
		return err
	}

	// TODO (wojciech): Try to reset map in single step
	delete(c.dirtyBlocks, meta)

	if meta.PostCommitFunc == nil {
		return nil
	}

	postCommitFunc := meta.PostCommitFunc
	meta.PostCommitFunc = nil

	return postCommitFunc()
}

func (c *Cache) fetchBlock(
	address blocks.BlockAddress,
	birthRevision uint64,
	nBytes int64,
	expectedChecksum blocks.Hash,
) (*metadata, error) {
	if address > c.singularityBlock.V.LastAllocatedBlock {
		return nil, errors.Errorf("block %d does not exist", address)
	}

	meta, err := c.findCachedBlock(address, birthRevision)
	if err != nil {
		return nil, err
	}

	if meta.State == usedBlockState {
		return meta, nil
	}
	if err := c.store.ReadBlock(address, meta.Data[:nBytes]); err != nil {
		return nil, err
	}
	if err := blocks.VerifyChecksum(address, meta.Data[:nBytes], expectedChecksum); err != nil {
		return nil, err
	}

	meta.State = usedBlockState

	return meta, nil
}

func (c *Cache) newBlock() (*metadata, error) {
	c.singularityBlock.V.LastAllocatedBlock++
	meta, err := c.findCachedBlock(c.singularityBlock.V.LastAllocatedBlock, c.singularityBlock.V.Revision+1)
	if err != nil {
		return nil, err
	}

	meta.State = usedBlockState
	c.dirtyBlocks[meta] = struct{}{}

	return meta, nil
}

func (c *Cache) findCachedBlock(address blocks.BlockAddress, birthRevision uint64) (*metadata, error) {
	// TODO (wojciech): Implement cache pruning based on hit metric

	cacheSeed := uint64(address) % c.nBlocks
	var found bool
	var selectedCacheIndex uint64
	var invalidCacheAddressFound bool
	var notReferencedAddressFound bool

loop:
	for i := uint64(0); i < c.nBlocks; i++ { // TODO (wojciech): Limit the number of tried offsets
		cacheIndex := (cacheSeed + c.addressingOffsets[i]) % c.nBlocks
		switch c.blocks[cacheIndex].State {
		case freeBlockState:
			if !invalidCacheAddressFound {
				found = true
				selectedCacheIndex = cacheIndex
			}
			break loop
		case invalidBlockState:
			if !invalidCacheAddressFound {
				invalidCacheAddressFound = true
				notReferencedAddressFound = true
				found = true
				selectedCacheIndex = cacheIndex
			}
		case usedBlockState:
			if c.blocks[cacheIndex].Address == address {
				found = true
				selectedCacheIndex = cacheIndex
				break loop
			}
			if !notReferencedAddressFound && c.blocks[cacheIndex].NReferences == 0 {
				notReferencedAddressFound = true
				found = true
				selectedCacheIndex = cacheIndex
			}
		}
	}

	if !found {
		return nil, errors.New("there are no free slots in cache")
	}

	meta := &c.blocks[selectedCacheIndex]
	if meta.State == usedBlockState && meta.Address != address {
		if _, exists := c.dirtyBlocks[meta]; exists {
			if err := c.commitBlock(meta); err != nil {
				return nil, err
			}
		}
		meta.State = invalidBlockState
	}

	if meta.State != usedBlockState {
		meta.PostCommitFunc = nil
	}

	meta.Address = address
	meta.BirthRevision = birthRevision

	return meta, nil
}

// FetchBlock returns structure representing existing block of particular type.
func FetchBlock[T blocks.Block](
	cache *Cache,
	pointer *pointer.Pointer,
) (Block[T], bool, error) {
	var v T
	meta, err := cache.fetchBlock(pointer.Address, pointer.BirthRevision, int64(unsafe.Sizeof(v)), pointer.Checksum)
	if err != nil {
		return Block[T]{}, false, err
	}

	return Block[T]{
		meta:  meta,
		Block: photon.NewFromBytes[T](meta.Data).V,
	}, meta.PostCommitFunc == nil, nil
}

// NewBlock returns structure representing new block of particular type.
func NewBlock[T blocks.Block](c *Cache) (Block[T], error) {
	meta, err := c.newBlock()
	if err != nil {
		return Block[T]{}, err
	}

	p := photon.NewFromBytes[T](meta.Data)

	// This is done because memory used for padding in structs is not zeroed automatically,
	// causing mismatch in hashes.
	copy(p.B, zeroContent)

	return Block[T]{
		meta:  meta,
		Block: p.V,
	}, nil
}

// DirtyBlock marks blocks as dirty.
func DirtyBlock[T blocks.Block](c *Cache, block Block[T]) error {
	c.dirtyBlocks[block.meta] = struct{}{}
	return nil
}

// InvalidateBlock marks block in cache as invalid.
func InvalidateBlock[T blocks.Block](c *Cache, block Block[T]) {
	delete(c.dirtyBlocks, block.meta)
	block.meta.State = invalidBlockState
	block.meta.PostCommitFunc = nil
}
