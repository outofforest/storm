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

// ReadBlock reads raw block bytes for the addressed block.
func (s *Store) ReadBlock(address types.BlockAddress, p []byte) error {
	if _, err := s.dev.Seek(int64(address)*types.BlockSize, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	n, err := s.dev.Read(p)
	if err != nil {
		return errors.WithStack(err)
	}
	if int64(n) > types.BlockSize {
		return errors.Errorf("expected to read %d bytes, but got %d", types.BlockSize, n)
	}
	return nil
}

func validateSingularityBlock(sBlock photon.Union[types.SingularityBlock]) error {
	if sBlock.V.StormID&stormSubject != stormSubject {
		return errors.New("device does not contain storm storage system")
	}

	storedChecksum := sBlock.V.Checksum
	sBlock.V.Checksum = types.Hash{}
	checksumComputed := types.Hash(sha256.Sum256(sBlock.B))

	if storedChecksum != checksumComputed {
		return errors.Errorf("checksum mismatch for the singularity block, computed: %s, stored: %s",
			hex.EncodeToString(checksumComputed[:]), hex.EncodeToString(storedChecksum[:]))
	}

	return nil
}
