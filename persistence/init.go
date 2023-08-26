package persistence

import (
	"io"
	"math/rand"

	"github.com/outofforest/photon"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/blocks"
	singularityV0 "github.com/outofforest/storm/blocks/singularity/v0"
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

	sBlock := photon.NewFromValue(&singularityV0.Block{
		SchemaVersion: blocks.SingularityV0,
		StormID:       rand.Uint64() | stormSubject,
		NBlocks:       uint64(dev.Size() / blocks.BlockSize),
	})
	sBlock.V.Checksum = sBlock.V.ComputeChecksum()

	if _, err := dev.Seek(0, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}

	if _, err := dev.Write(sBlock.B); err != nil {
		return errors.WithStack(err)
	}

	// TODO (wojciech): After syncing, open dev using O_DIRECT option to verify that data have been written correctly.
	return dev.Sync()
}

func validateDev(dev Dev, overwrite bool) error {
	size := dev.Size()
	nBlocks := uint64(size / blocks.BlockSize)

	if nBlocks < minNBlocks {
		return errors.Errorf("device is too small, minimum size is: %d bytes, provided: %d", minNBlocks*blocks.BlockSize, size)
	}

	_, sBlock, err := loadSingularityBlock(dev)
	if err != nil {
		return err
	}

	if sBlock.StormID&stormSubject == stormSubject && !overwrite {
		return errors.WithStack(ErrAlreadyInitialized)
	}

	return nil
}

//nolint:unparam // returned bock address won't be always 0 in the future
func loadSingularityBlock(dev Dev) (blocks.BlockAddress, singularityV0.Block, error) {
	var address blocks.BlockAddress
	if _, err := dev.Seek(int64(address)*blocks.BlockSize, io.SeekStart); err != nil {
		return 0, singularityV0.Block{}, errors.WithStack(err)
	}

	sBlock := photon.NewFromBytes[singularityV0.Block](make([]byte, blocks.BlockSize))
	if _, err := dev.Read(sBlock.B); err != nil {
		return 0, singularityV0.Block{}, errors.WithStack(err)
	}

	return address, *sBlock.V, nil
}
