package main

import (
	"os"

	"github.com/funkygao/go-helix/apps/fedis/command"
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

		"scale": func() (cli.Command, error) {
			return &command.Scale{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"redis": func() (cli.Command, error) {
			return &command.Redis{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"trace": func() (cli.Command, error) {
			return &command.Trace{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"status": func() (cli.Command, error) {
			return &command.Status{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
