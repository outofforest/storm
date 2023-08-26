package persistence

import (
	"io"

	"github.com/outofforest/photon"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/blocks"
	singularityV0 "github.com/outofforest/storm/blocks/singularity/v0"
)

// Store represents persistent storage.
type Store struct {
	dev Dev
}

// OpenStore opens the persistent store.
func OpenStore(dev Dev) (*Store, error) {
	address, sBlock, err := loadSingularityBlock(dev)
	if err != nil {
		return nil, err
	}
	if err := validateSingularityBlock(address, sBlock); err != nil {
		return nil, err
	}
	availableBlocks := uint64(dev.Size() / blocks.BlockSize)
	if availableBlocks < sBlock.NBlocks {
		return nil, errors.Errorf("singularity block reports %d blocks, but only %d are available on the device",
			sBlock.NBlocks, availableBlocks)
	}

	return &Store{
		dev: dev,
	}, nil
}

// ReadBlock reads raw block bytes from the addressed block.
func (s *Store) ReadBlock(address blocks.BlockAddress, p []byte) error {
	if len(p) == 0 || int64(len(p)) > blocks.BlockSize {
		return errors.Errorf("invalid size of output buffer: %d", len(p))
	}

	if _, err := s.dev.Seek(int64(address)*blocks.BlockSize, io.SeekStart); err != nil {
		return err
	}
	if _, err := s.dev.Read(p); err != nil {
		return err
	}
	return nil
}

// WriteBlock writes raw block bytes to the addressed block.
func (s *Store) WriteBlock(address blocks.BlockAddress, p []byte) error {
	if len(p) == 0 || int64(len(p)) > blocks.BlockSize {
		return errors.Errorf("invalid size of input buffer: %d", len(p))
	}

	if _, err := s.dev.Seek(int64(address)*blocks.BlockSize, io.SeekStart); err != nil {
		return err
	}
	if _, err := s.dev.Write(p); err != nil {
		return err
	}
	return nil
}

// Sync forces data to be written to the dev.
func (s *Store) Sync() error {
	return s.dev.Sync()
}

func validateSingularityBlock(address blocks.BlockAddress, sBlock singularityV0.Block) error {
	if sBlock.StormID&stormSubject != stormSubject {
		return errors.New("device does not contain storm storage system")
	}

	sBlock2 := sBlock
	sBlock2.Checksum = 0
	return blocks.VerifyChecksum(address, photon.NewFromValue(&sBlock2).B, sBlock.Checksum)
}
