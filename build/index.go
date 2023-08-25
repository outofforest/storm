package build

import (
	"github.com/outofforest/build"
	"github.com/outofforest/buildgo"
)

// Commands is a definition of commands available in build system
var Commands = map[string]build.Command{
	"setup": {Fn: setup, Description: "Installs tools required by development environment"},
}

func init() {
	buildgo.AddCommands(Commands)
	Commands["dev/test"] = build.Command{Fn: goTests, Description: "Runs go unit tests"}
}
