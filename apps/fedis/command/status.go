package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	golog "log"
	"sort"
	"strings"

	"github.com/funkygao/go-helix/store/zk"
	"github.com/funkygao/gocli"
	log "github.com/funkygao/log4go"
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

	golog.SetOutput(ioutil.Discard)
	log.Disable()

	// create the admin instance and connect
	admin := zk.NewZkHelixAdmin(zkSvr)
	must(admin.Connect())

	controller := admin.ControllerLeader(cluster)
	controlerHistory, err := admin.ControllerHistory(cluster)
	must(err)

	instances, err := admin.Instances(cluster)
	must(err)

	liveInstances, err := admin.LiveInstances(cluster)
	must(err)

	resources, err := admin.Resources(cluster)
	must(err)

	view, err := admin.ResourceExternalView(cluster, resource)
	must(err)

	// controller
	this.Ui.Outputf("controller: %s", controller)
	this.Ui.Outputf("controller history:")
	this.Ui.Outputf("    %+v", controlerHistory)

	// resource
	this.Ui.Outputf("resources:")
	for _, res := range resources {
		idealState, err := admin.ResourceIdealState(cluster, res)
		must(err)
		this.Ui.Outputf("    %s#%d replica:%s %s %s", idealState.Resource(),
			idealState.NumPartitions(),
			idealState.Replicas(),
			idealState.RebalanceMode(),
			idealState.StateModelDefRef(),
		)
	}

	this.Ui.Info("external view:")
	var sortedPartitions []string
	for p := range view.MapFields {
		sortedPartitions = append(sortedPartitions, p)
	}
	sort.Strings(sortedPartitions)
	for _, partition := range sortedPartitions {
		m := view.MapFields[partition]
		for instance, state := range m {
			this.Ui.Outputf("    %s %s %s", partition, instance, state)
		}
	}

	// instance
	this.Ui.Outputf("instances:")
	for _, instance := range instances {
		ic, err := admin.InstanceConfig(cluster, instance)
		must(err)

		this.Ui.Outputf("    %s", ic)
	}

	// live instances
	this.Ui.Outputf("live instances:")
	for _, instance := range liveInstances {
		this.Ui.Infof("    %s", instance)
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
