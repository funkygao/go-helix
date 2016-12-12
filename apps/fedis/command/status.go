package command

import (
	"flag"
	"fmt"
	"strings"

	//"github.com/funkygao/go-helix"
	//"github.com/funkygao/go-helix/model"
	"github.com/funkygao/go-helix/store/zk"
	"github.com/funkygao/gocli"
)

type Status struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Status) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("status", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	// create the admin instance and connect
	admin := zk.NewZkHelixAdmin(zkSvr)
	must(admin.Connect())

	instances, resources, err := admin.ClusterInfo(cluster)
	must(err)

	this.Ui.Output(fmt.Sprintf("resources: %+v", resources))
	this.Ui.Output(fmt.Sprintf("instances: %+v", instances))

	return
}

func (*Status) Synopsis() string {
	return "Display current redis cluster status"
}

func (this *Status) Help() string {
	help := fmt.Sprintf(`
Usage: %s status [options]

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
