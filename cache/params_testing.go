//go:build test

package cache

const (
	// MaxCacheTries is the maximum number of probes using open addressing before taking over a block in cache.
	MaxCacheTries = 2

	// MaxDirtyBlocks is the number of maximum dirty blocks triggering a commit.
	MaxDirtyBlocks = 2
)
