package blocks

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/pkg/errors"
)

// Checksum computes checksum of bytes.
func Checksum(b []byte) Hash {
	return sha256.Sum256(b)
}

// VerifyChecksum verifies that checksum of provided data matches the expected one.
func VerifyChecksum(address BlockAddress, p []byte, expectedChecksum Hash) error {
	checksum := Checksum(p)
	if checksum == expectedChecksum {
		return nil
	}
	return errors.Errorf("checksum mismatch for block %d, computed: %s, expected: %s",
		address, hex.EncodeToString(checksum[:]), hex.EncodeToString(expectedChecksum[:]))
}
