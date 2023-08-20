package persistence

import (
	"crypto/sha256"
	"io"
	"math/rand"

	"github.com/outofforest/photon"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/types"
)

const (
	// minBlocks specifies the minimum amount of blocks which must fit into device, this is an arbitrary number coming from my head.
	minNBlocks = 32

	// stormSubject defines an identifier used to detect if storm system exists on the device.
	stormSubject = 0b0100001000000000100000010010000100010010110000100010010001000101
)

// Dev is the interface required from the device.
type Dev interface {
	io.ReadWriteSeeker
	Sync() error
	Size() int64
}

// ErrAlreadyInitialized is returned if during initialization, another storm instance is detected on the device.
var ErrAlreadyInitialized = errors.New("storm has been already initialized on the provided device")

// Initialize initializes new storm storage.
func Initialize(dev Dev, overwrite bool) error {
	// TODO (wojciech): Store many copies of the singularity block

	if err := validateDev(dev, overwrite); err != nil {
		return err
	}

	rootDataBlock := photon.NewFromValue(&types.PointerBlock{})

	sBlock := photon.NewFromValue(&types.SingularityBlock{
		StormID:            rand.Uint64() | stormSubject,
		NBlocks:            uint64(dev.Size() / types.BlockSize),
		LastAllocatedBlock: 1,
		RootData: types.Pointer{
			StructChecksum: sha256.Sum256(rootDataBlock.B),
			DataChecksum:   pointerBlockDataChecksum(rootDataBlock.V),
			Address:        1,
		},
	})
	sBlock.V.StructChecksum = sha256.Sum256(sBlock.B)

	if _, err := dev.Seek(0, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}

	if _, err := dev.Write(sBlock.B); err != nil {
		return errors.WithStack(err)
	}

	if _, err := dev.Seek(types.BlockSize, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}

	if _, err := dev.Write(rootDataBlock.B); err != nil {
		return errors.WithStack(err)
	}

	// TODO (wojciech): After syncing, open dev using O_DIRECT option to verify that data have been written correctly.
	return dev.Sync()
}

func validateDev(dev Dev, overwrite bool) error {
	size := dev.Size()
	nBlocks := uint64(size / types.BlockSize)

	if nBlocks < minNBlocks {
		return errors.Errorf("device is too small, minimum size is: %d bytes, provided: %d", minNBlocks*types.BlockSize, size)
	}

	sBlock, err := loadSingularityBlock(dev)
	if err != nil {
		return err
	}

	if sBlock.V.StormID&stormSubject == stormSubject && !overwrite {
		return errors.WithStack(ErrAlreadyInitialized)
	}

	return nil
}

func loadSingularityBlock(dev Dev) (photon.Union[types.SingularityBlock], error) {
	if _, err := dev.Seek(0, io.SeekStart); err != nil {
		return photon.Union[types.SingularityBlock]{}, errors.WithStack(err)
	}

	sBlock := photon.NewFromBytes[types.SingularityBlock](make([]byte, types.BlockSize))
	if _, err := dev.Read(sBlock.B); err != nil {
		return photon.Union[types.SingularityBlock]{}, errors.WithStack(err)
	}

	return sBlock, nil
}

func pointerBlockDataChecksum(pBlock *types.PointerBlock) types.Hash {
	hasher := sha256.New()

	for i, state := range pBlock.PointedBlockTypes {
		if state != types.FreeBlockType {
			hasher.Write(pBlock.Pointers[i].DataChecksum[:])
		}
	}

	return types.Hash(hasher.Sum(nil))
}
