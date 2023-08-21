package persistence

import (
	"encoding/hex"
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
	sBlock, err := loadSingularityBlock(dev)
	if err != nil {
		return nil, err
	}
	if err := validateSingularityBlock(sBlock); err != nil {
		return nil, err
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
		return errors.WithStack(err)
	}
	if _, err := s.dev.Read(p); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// WriteBlock writes raw block bytes to the addressed block.
func (s *Store) WriteBlock(address blocks.BlockAddress, p []byte) error {
	if len(p) == 0 || int64(len(p)) > blocks.BlockSize {
		return errors.Errorf("invalid size of input buffer: %d", len(p))
	}

	if _, err := s.dev.Seek(int64(address)*blocks.BlockSize, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	if _, err := s.dev.Write(p); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Sync forces data to be written to the dev.
func (s *Store) Sync() error {
	return errors.WithStack(s.dev.Sync())
}

func validateSingularityBlock(sBlock photon.Union[singularityV0.Block]) error {
	if sBlock.V.StormID&stormSubject != stormSubject {
		return errors.New("device does not contain storm storage system")
	}

	checksumComputed := sBlock.V.ComputeChecksum()
	if sBlock.V.Checksum != checksumComputed {
		return errors.Errorf("checksum mismatch for the singularity block, computed: %s, stored: %s",
			hex.EncodeToString(checksumComputed[:]), hex.EncodeToString(sBlock.V.Checksum[:]))
	}

	return nil
}
