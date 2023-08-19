package persistence

import (
	"crypto/sha256"
	"encoding/hex"
	"io"

	"github.com/outofforest/photon"
	"github.com/pkg/errors"

	"github.com/outofforest/storm/types"
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
func (s *Store) ReadBlock(address types.BlockAddress, p []byte) error {
	if len(p) == 0 || int64(len(p)) > types.BlockSize {
		return errors.Errorf("invalid size of output buffer: %d", len(p))
	}

	if _, err := s.dev.Seek(int64(address)*types.BlockSize, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	if _, err := s.dev.Read(p); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// WriteBlock writes raw block bytes to the addressed block.
func (s *Store) WriteBlock(address types.BlockAddress, p []byte) error {
	if len(p) == 0 || int64(len(p)) > types.BlockSize {
		return errors.Errorf("invalid size of input buffer: %d", len(p))
	}

	if _, err := s.dev.Seek(int64(address)*types.BlockSize, io.SeekStart); err != nil {
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

func validateSingularityBlock(sBlock photon.Union[types.SingularityBlock]) error {
	if sBlock.V.StormID&stormSubject != stormSubject {
		return errors.New("device does not contain storm storage system")
	}

	storedChecksum := sBlock.V.StructChecksum
	sBlock.V.StructChecksum = types.Hash{}
	checksumComputed := types.Hash(sha256.Sum256(sBlock.B))

	if storedChecksum != checksumComputed {
		return errors.Errorf("checksum mismatch for the singularity block, computed: %s, stored: %s",
			hex.EncodeToString(checksumComputed[:]), hex.EncodeToString(storedChecksum[:]))
	}

	return nil
}
