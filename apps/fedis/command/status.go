package command

import (
	"flag"
	"fmt"
	"strings"

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

	controller := admin.ControllerLeader(cluster)
	controlerHistory, err := admin.ControllerHistory(cluster)
	must(err)

	instances, err := admin.Instances(cluster)
	must(err)

	resources, err := admin.Resources(cluster)
	must(err)

	// controller
	this.Ui.Info(fmt.Sprintf("controller: %s", controller))
	this.Ui.Output(fmt.Sprintf("    %+v", controlerHistory))

	// resource
	this.Ui.Info(fmt.Sprintf("resources:"))
	for _, res := range resources {
		idealState, err := admin.ResourceIdealState(cluster, res)
		must(err)
		this.Ui.Output(fmt.Sprintf("    %s#%d replica:%s %s %s", idealState.Resource(),
			idealState.NumPartitions(),
			idealState.Replicas(),
			idealState.RebalanceMode(),
			idealState.StateModelDefRef(),
		))
	}

	// instance
	this.Ui.Info(fmt.Sprintf("instances:"))
	for _, instance := range instances {
		ic, err := admin.InstanceConfig(cluster, instance)
		must(err)

		this.Ui.Output(fmt.Sprintf("    %s", ic))
	}

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
