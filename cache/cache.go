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
	nBlocks     uint64
	data        []byte
	blocks      []block
	dirtyBlocks map[uint64]struct{}

	singularityBlock photon.Union[*singularityV0.Block]
}

// New creates new cache.
func New(store *persistence.Store, size int64) (*Cache, error) {
	sBlock := photon.NewFromValue(&singularityV0.Block{})
	if err := store.ReadBlock(0, sBlock.B); err != nil {
		return nil, err
	}

	nBlocks := uint64(size) / uint64(blocks.BlockSize)
	data := make([]byte, nBlocks*uint64(blocks.BlockSize))
	bs := make([]block, nBlocks)
	for i, offset := 0, int64(0); i < len(bs); i, offset = i+1, offset+blocks.BlockSize {
		bs[i].Data = data[offset : offset+blocks.BlockSize]
	}
	return &Cache{
		store:            store,
		nBlocks:          nBlocks,
		data:             data,
		blocks:           bs,
		dirtyBlocks:      make(map[uint64]struct{}, MaxDirtyBlocks),
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
	for cacheIndex := range c.dirtyBlocks {
		block := c.blocks[cacheIndex]
		if err := c.store.WriteBlock(block.Address, block.Data); err != nil {
			return err
		}
		c.blocks[cacheIndex].State = fetchedBlockState
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
	birthRevision uint64,
	nBytes int64,
	expectedChecksum blocks.Hash,
) (*block, error) {
	if address > c.singularityBlock.V.LastAllocatedBlock {
		return nil, errors.Errorf("block %d does not exist", address)
	}

	cacheIndex, err := c.findCachedBlock(address)
	if err != nil {
		return nil, err
	}

	b := &c.blocks[cacheIndex]
	if b.State == fetchedBlockState || b.State == newBlockState {
		return b, nil
	}

	b.Address = address
	b.State = fetchedBlockState

	if err := c.store.ReadBlock(address, b.Data[:nBytes]); err != nil {
		return nil, err
	}
	if err := blocks.VerifyChecksum(address, b.Data[:nBytes], expectedChecksum); err != nil {
		return nil, err
	}

	// This means that block is freshly created but was written to persistent store due to lack of space in cache.
	// In this is case it is marked as new to avoid useless copying.
	if birthRevision > c.singularityBlock.V.Revision {
		b.State = newBlockState
	}

	return b, nil
}

func (c *Cache) copyBlock(
	address blocks.BlockAddress,
	birthRevision uint64,
	nBytes int64,
	expectedChecksum blocks.Hash,
) (*block, error) {
	b, err := c.fetchBlock(address, birthRevision, nBytes, expectedChecksum)
	if err != nil {
		return nil, err
	}

	if b.State == newBlockState {
		return b, nil
	}

	b.State = invalidBlockState

	b2, err := c.newBlock()
	if err != nil {
		return nil, err
	}

	b2.Data, b.Data = b.Data, b2.Data

	return b2, nil
}

func (c *Cache) newBlock() (*block, error) {
	address := c.singularityBlock.V.LastAllocatedBlock + 1
	cacheIndex, err := c.findCachedBlock(address)
	if err != nil {
		return nil, err
	}
	c.singularityBlock.V.LastAllocatedBlock++

	b := &c.blocks[cacheIndex]
	b.Address = address
	b.State = newBlockState

	if len(c.dirtyBlocks) >= MaxDirtyBlocks {
		if err := c.commitData(); err != nil {
			return nil, err
		}
	}
	c.dirtyBlocks[cacheIndex] = struct{}{}

	return b, nil
}

func (c *Cache) findCachedBlock(address blocks.BlockAddress) (uint64, error) {
	// TODO (wojciech): Implement cache pruning based on hit metric

	// For now, if there is no free space in cache found in `maxCacheTries` tries, the first tried block is replaced
	// by the fetched one.
	selectedCacheIndex := uint64(address) % c.nBlocks
	var invalidCacheAddressFound bool

	// Multiplying by 3 instead of 2 here is better, because it produces both even and odd addresses.
	for i, cacheIndex := 0, selectedCacheIndex; i < MaxCacheTries; i, cacheIndex = i+1, (cacheIndex*3)%c.nBlocks {
		switch c.blocks[cacheIndex].State {
		case freeBlockState:
			if invalidCacheAddressFound {
				return selectedCacheIndex, nil
			}
			return cacheIndex, nil
		case invalidBlockState:
			if !invalidCacheAddressFound {
				invalidCacheAddressFound = true
				selectedCacheIndex = cacheIndex
			}
		case fetchedBlockState, newBlockState:
			if c.blocks[cacheIndex].Address == address {
				return cacheIndex, nil
			}
		}
	}

	switch c.blocks[selectedCacheIndex].State {
	case newBlockState:
		if err := c.store.WriteBlock(c.blocks[selectedCacheIndex].Address, c.blocks[selectedCacheIndex].Data); err != nil {
			return 0, err
		}
		delete(c.dirtyBlocks, selectedCacheIndex)
		c.blocks[selectedCacheIndex].State = invalidBlockState
	case fetchedBlockState:
		c.blocks[selectedCacheIndex].State = invalidBlockState
	}

	return selectedCacheIndex, nil
}

// FetchBlock returns structure representing existing block of particular type.
func FetchBlock[T blocks.Block](
	cache *Cache,
	pointer pointerV0.Pointer,
) (*T, blocks.BlockAddress, error) {
	var v T
	block, err := cache.fetchBlock(pointer.Address, pointer.BirthRevision, int64(unsafe.Sizeof(v)), pointer.Checksum)
	if err != nil {
		return nil, 0, err
	}

	return photon.NewFromBytes[T](block.Data).V, block.Address, nil
}

// CopyBlock returns a copy of the block of particular type.
func CopyBlock[T blocks.Block](
	cache *Cache,
	pointer pointerV0.Pointer,
) (*T, blocks.BlockAddress, error) {
	var v T
	block, err := cache.copyBlock(pointer.Address, pointer.BirthRevision, int64(unsafe.Sizeof(v)), pointer.Checksum)
	if err != nil {
		return nil, 0, err
	}

	return photon.NewFromBytes[T](block.Data).V, block.Address, nil
}

// NewBlock returns structure representing new block of particular type.
func NewBlock[T blocks.Block](cache *Cache) (*T, blocks.BlockAddress, error) {
	block, err := cache.newBlock()
	if err != nil {
		return nil, 0, err
	}

	p := photon.NewFromBytes[T](block.Data)

	// This is done because memory used for padding in structs is not zeroed automatically,
	// causing mismatch in hashes.
	copy(p.B, zeroContent)

	return p.V, block.Address, nil
}
