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
	cmdFlags := flag.NewFlagSet("scale", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	// create the admin instance and connect
	admin := zk.NewZKHelixAdmin(zkSvr)
	must(admin.Connect())

	instances, err := admin.Instances(cluster)
	must(err)
	for _, instance := range instances {
		this.Ui.Info(instance)
	}

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

    -z zone

    -add cluster

    -drop cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
