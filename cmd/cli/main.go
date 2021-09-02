package main

import (
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	os.Setenv("PROJECT_ID", "loupe-dev")
	os.Setenv("PROJECT", "local")
	os.Setenv("SERVER_NAME", "orchard-cli")
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
		Commands: []*cli.Command{},
	}
	return app
}
