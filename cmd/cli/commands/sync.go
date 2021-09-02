package commands

import (
	"github.com/urfave/cli/v2"
)

func GetSyncCommand() *cli.Command {
	return &cli.Command{
		Name:  "sync",
		Usage: "sync various user/group data to/from orchard",
		Subcommands: []*cli.Command{
			GetImportAuth0Command(),
		},
	}
}
