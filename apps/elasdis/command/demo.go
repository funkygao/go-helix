package command

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/store/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
)

const (
	zkSvr            = "localhost:2181"
	cluster          = "foobar"
	node1            = "localhost_10925"
	node2            = "localhost_10926"
	stateModel       = helix.StateModelOnlineOffline
	resource         = "redis"
	partitions       = 3
	replicas         = "2"
	helixInstallBase = "/opt/helix"
)

type Demo struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Demo) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("demo", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	// create the admin instance and connect
	admin := zk.NewZKHelixAdmin(zkSvr)
	must(admin.Connect())

	// create the cluster
	must(admin.AddCluster(cluster))
	defer func() {
		log.Info("drop cluster %s", cluster)
		admin.DropCluster(cluster)
		log.Close()
	}()
	log.Info("added cluster: %s", cluster)

	must(admin.AllowParticipantAutoJoin(cluster, false))

	// add 2 nodes to the cluster
	must(admin.AddNode(cluster, node1))
	must(admin.AddNode(cluster, node2))
	log.Info("node: %s %s added to cluster[%s]", node1, node2, cluster)

	// define the resource and partition
	resourceOption := helix.DefaultAddResourceOption(partitions, stateModel)
	must(admin.AddResource(cluster, resource, resourceOption))
	log.Info("resource[%s] partitions:%d model:%s added to cluster[%s]", resource,
		partitions, stateModel, cluster)

	// create the manager instance and connect
	manager := zk.NewZKHelixManager(zkSvr, zk.WithSessionTimeout(time.Second*10))
	must(manager.Connect())

	// the actual task executor
	participant := manager.NewParticipant(cluster, "localhost", "10925")
	sm := helix.NewStateModel()
	sm.AddTransitions([]helix.Transition{
		{"ONLINE", "OFFLINE", func(message *helix.Message, context *helix.Context) {
			log.Info(color.Green("resource[%s] partition[%s] ONLINE-->OFFLINE",
				message.Resource(),
				message.PartitionName()))
		}},

		{"OFFLINE", "ONLINE", func(message *helix.Message, context *helix.Context) {
			log.Info(color.Cyan("resource[%s] partition[%s] OFFLINE-->ONLINE",
				message.Resource(),
				message.PartitionName()))
		}},
	})
	participant.RegisterStateModel(stateModel, sm)

	must(participant.Start())
	log.Info("participant started")

	instances, _ := admin.Instances(cluster)
	log.Info("instances: %+v", instances)

	log.Info("start rebalancing...")
	helix.Rebalance(zkSvr, cluster, resource, replicas)
	log.Info("rebalanced done")

	log.Info("waiting Ctrl-C...")

	time.Sleep(time.Second * 5)
	participant.Close()

	log.Info("%s/bin/run-helix-controller.sh --zkSvr %s --cluster %s", helixInstallBase, zkSvr, cluster)

	// block until SIGINT and SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	return
}

func (*Demo) Synopsis() string {
	return "Demonstration how to use Helix to control redis instances"
}

func (this *Demo) Help() string {
	help := fmt.Sprintf(`
Usage: %s demo [options]

    %s

Options:

    -z zone

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
