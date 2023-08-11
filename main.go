package main

import (
	csa "cmps/app"
	"fmt"
	"os"
	"runtime"

	"github.com/urfave/cli"
)

const Version = "v0.1.0"

func main() {
	if err := setupApp().Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func setupApp() *cli.App {
	app := cli.NewApp()
	app.Usage = "CMP Server"
	app.Action = func(c *cli.Context) { csa.Run(c) }
	app.Version = Version
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: "./config/",
			Usage: "config director for the application",
		}}
	app.Commands = []cli.Command{}
	app.Before = func(_ *cli.Context) error {
		runtime.GOMAXPROCS(runtime.NumCPU())
		return nil
	}
	return app
}
