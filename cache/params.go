//go:build !test

package cache

import pointerV0 "github.com/outofforest/storm/blocks/pointer/v0"

const (
	// MaxCacheTries is the maximum number of probes using open addressing before taking over a block in cache.
	MaxCacheTries = 5

	// MaxDirtyBlocks is the number of maximum dirty blocks triggering a commit.
	MaxDirtyBlocks = 2 * pointerV0.PointersPerBlock
)
