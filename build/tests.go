package build

import (
	"context"

	"github.com/outofforest/build"
	"github.com/outofforest/buildgo"
)

func goTests(ctx context.Context, deps build.DepsFunc) error {
	return buildgo.GoTest(ctx, deps, "test")
}
