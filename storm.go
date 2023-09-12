package storm

import (
	"github.com/outofforest/storm/blocks"
	"github.com/outofforest/storm/cache"
	"github.com/outofforest/storm/keystore"
	"github.com/outofforest/storm/objectstore"
	"github.com/outofforest/storm/spacestore"
)

// Storm is used to access objects stored in space.
type Storm[T comparable] struct {
	c               *cache.Cache
	spaceID         blocks.SpaceID
	objectsPerBlock int
}

// New returns new storm instance.
func New[T comparable](
	c *cache.Cache,
	spaceID blocks.SpaceID,
	objectsPerBlock int,
) *Storm[T] {
	return &Storm[T]{
		c:               c,
		spaceID:         spaceID,
		objectsPerBlock: objectsPerBlock,
	}
}

// Get returns object by key.
func (s *Storm[T]) Get(key []byte) (T, bool, error) {
	sBlock := s.c.SingularityBlock()
	spaceOrigin := cache.BlockOrigin{
		Pointer:   &sBlock.SpacePointer,
		BlockType: &sBlock.SpaceBlockType,
	}
	keyOrigin, objectOrigin, exists, err := spacestore.GetSpace(s.c, spaceOrigin, s.spaceID)
	if !exists || err != nil {
		var t T
		return t, false, err
	}

	objectID, objectExists, err := keystore.GetObjectID(s.c, keyOrigin, key)
	if !objectExists || err != nil {
		var t T
		return t, false, err
	}

	value, valueExists, err := objectstore.GetObject[T](s.c, objectOrigin, s.objectsPerBlock, objectID)
	// TODO (wojciech): Return error if `valueExists` is false
	if !valueExists || err != nil {
		var t T
		return t, false, err
	}

	return value, true, nil
}

// Set sets object under key.
func (s *Storm[T]) Set(key []byte, object T) error {
	sBlock := s.c.SingularityBlock()
	keyOrigin, objectOrigin, spaceTrace, err := spacestore.EnsureSpace(s.c, cache.BlockOrigin{
		Pointer:   &sBlock.SpacePointer,
		BlockType: &sBlock.SpaceBlockType,
	}, s.spaceID)
	if err != nil {
		return err
	}

	objectID, err := keystore.EnsureObjectID(s.c, keyOrigin, spaceTrace, key)
	if err != nil {
		return err
	}

	return objectstore.SetObject[T](s.c, objectOrigin, spaceTrace, s.objectsPerBlock, objectID, object)
}
