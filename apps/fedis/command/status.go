package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/go-helix"
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

	// create the admin instance and connect
	admin := zk.NewZKHelixAdmin(zkSvr)
	must(admin.Connect())

	// create the cluster
	must(admin.AddCluster(cluster))
	log.Info("added cluster: %s", cluster)

	must(admin.AllowParticipantAutoJoin(cluster, true))

	// define the resource and partition
	resourceOption := helix.DefaultAddResourceOption(partitions, stateModel)
	resourceOption.RebalancerMode = helix.RebalancerModeFullAuto
	must(admin.AddResource(cluster, resource, resourceOption))
	log.Info("resource[%s] partitions:%d model:%s added to cluster[%s]", resource,
		partitions, stateModel, cluster)

	log.Info("%s/bin/run-helix-controller.sh --zkSvr %s --cluster %s", helixInstallBase, zkSvr, cluster)

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
