package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Router struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Router) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("router", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return
}

func (*Router) Synopsis() string {
	return "Dispatch redis commands to a proper redislet"
}

func (this *Router) Help() string {
	help := fmt.Sprintf(`
Usage: %s router [options]

    %s

Options:

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
