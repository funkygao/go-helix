package main

import (
	"os"

	"github.com/funkygao/go-helix/apps/dfs/command"
	"github.com/funkygao/gocli"
)

var commands map[string]cli.CommandFactory

func init() {
	ui := &cli.ColoredUi{
		Ui: &cli.BasicUi{
			Writer:      os.Stdout,
			Reader:      os.Stdin,
			ErrorWriter: os.Stderr,
		},

		OutputColor: cli.UiColorNone,
		InfoColor:   cli.UiColorGreen,
		ErrorColor:  cli.UiColorRed,
		WarnColor:   cli.UiColorYellow,
	}
	cmd := os.Args[0]

	commands = map[string]cli.CommandFactory{
		"init": func() (cli.Command, error) {
			return &command.Init{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"start": func() (cli.Command, error) {
			return &command.Start{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
