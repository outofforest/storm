package blocks

import (
	"github.com/cespare/xxhash/v2"
	"github.com/outofforest/photon"
	"github.com/pkg/errors"
)

// BlockChecksum computes checksum of the block.
func BlockChecksum[T Block](b *T) Hash {
	return Checksum(photon.NewFromValue(b).B)
}

// Checksum computes checksum of bytes.
func Checksum(b []byte) Hash {
	return Hash(xxhash.Sum64(b))
}

// VerifyChecksum verifies that checksum of provided data matches the expected one.
func VerifyChecksum(address BlockAddress, p []byte, expectedChecksum Hash) error {
	checksum := Checksum(p)
	if checksum == expectedChecksum {
		return nil
	}
	return errors.Errorf("checksum mismatch for block %d, computed: %#v, expected: %#v",
		address, checksum, expectedChecksum)
}
