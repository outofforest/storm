package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const alignment = 8

func TestHeaderSizeIsAligned(t *testing.T) {
	assert.EqualValues(t, 0, CacheHeaderSize%alignment)
}
