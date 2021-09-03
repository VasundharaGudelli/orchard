package main

import (
	"log"
	"os"

	"github.com/loupe-co/orchard/cmd/cli/commands"
	"github.com/urfave/cli/v2"
)

func main() {
	os.Setenv("PROJECT_ID", "loupe-dev")
	os.Setenv("PROJECT", "local")
	os.Setenv("SERVER_NAME", "orchard-cli")
	os.Setenv("AUTH_0_ISSUER", "loupe-dev.auth0.com")
	os.Setenv("AUTH_0_AUDIENCE", "Fb8FuT6ezfLFG2tabZeFh2r8NsTD4AAm")
	os.Setenv("AUTH_0_DOMAIN", "https://loupe-dev.auth0.com")
	os.Setenv("AUTH_0_CLIENT_ID", "DNGDG7ypZ1aCm98y2SGImpHIEexPTwDP")
	os.Setenv("AUTH_0_CLIENT_SECRET", "p6v0GpkOpDJHC3TatCHBHXbUj9QmaKZlP2wIW8ljWNlFyI32ex_dT7YwkYzNwpik")
	if err := getApp().Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func getApp() *cli.App {
	app := &cli.App{
		Name:  "orchard",
		Usage: "manage orchard group and person data in sql",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "env",
				Aliases: []string{"e"},
				Usage:   "set env for cli execution. currently supports 'dev' and 'prod'",
				Value:   "dev",
			},
		},
		Commands: []*cli.Command{
			commands.GetSyncCommand(),
			commands.GetUpdateGroupTypesCommand(),
		},
	}
	return app
}
