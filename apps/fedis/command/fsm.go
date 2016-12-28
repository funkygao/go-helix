package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/go-helix/apps/fedis/command/redis"
	"github.com/funkygao/gocli"
)

type Fsm struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Fsm) Run(args []string) (exitCode int) {
	var outfile string
	cmdFlags := flag.NewFlagSet("fsm", flag.ContinueOnError)
	cmdFlags.StringVar(&outfile, "o", "fsm.png", "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	r := redis.NewNode(zkSvr, cluster, resource, "", 0, "", "")
	sm := r.StateModel()
	if sm != nil {
		must(sm.ExportDiagram(outfile))
	}
	this.Ui.Infof("check out %s", outfile)

	return
}

func (*Fsm) Synopsis() string {
	return "Export redis FSM state diagram to png file"
}

func (this *Fsm) Help() string {
	help := fmt.Sprintf(`
Usage: %s fsm [options]

    %s

Options:

    -o outfile

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
