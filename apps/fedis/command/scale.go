package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/go-helix/store/zk"
	"github.com/funkygao/gocli"
)

type Scale struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Scale) Run(args []string) (exitCode int) {
	var partition int
	cmdFlags := flag.NewFlagSet("scale", flag.ContinueOnError)
	cmdFlags.IntVar(&partition, "p", 0, "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if partition == 0 {
		this.Ui.Output(this.Help())
		return 2
	}

	admin := zk.NewZkHelixAdmin(zkSvr)
	must(admin.Connect())

	must(admin.ScaleResource(cluster, resource, partition))
	this.Ui.Info(fmt.Sprintf("scaled to %d", partition))

	return
}

func (*Scale) Synopsis() string {
	return "Scale up/down redis instances"
}

func (this *Scale) Help() string {
	help := fmt.Sprintf(`
Usage: %s scale [options]

    %s

Options:

    -p partition

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
