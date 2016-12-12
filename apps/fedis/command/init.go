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

type Init struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Init) Run(args []string) (exitCode int) {
	var node string
	cmdFlags := flag.NewFlagSet("init", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&node, "addNode", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	// create the admin instance and connect
	admin := zk.NewZkHelixAdmin(zkSvr)
	must(admin.Connect())

	if node != "" {
		must(admin.AddNode(cluster, node))
		log.Info("node:%s added to cluster[%s]", node, cluster)
		return
	}

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

func (*Init) Synopsis() string {
	return "Initialize redis cluster"
}

func (this *Init) Help() string {
	help := fmt.Sprintf(`
Usage: %s init [options]

    %s

Options:

    -addNode host_port

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
