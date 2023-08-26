package blocks

import (
	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
)

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
	return errors.Errorf("checksum mismatch for block %d, computed: %d, expected: %d",
		address, checksum, expectedChecksum)
}
