package command

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/store/zk"
	"github.com/funkygao/gocli"
)

const (
	zkSvr      = "localhost:2181"
	cluster    = "foobar"
	node1      = "localhost_10925"
	node2      = "localhost_10926"
	stateModel = helix.StateModelOnlineOffline
	resource   = "redis"
	partitions = 5
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

	admin := zk.NewZKHelixAdmin(zkSvr)
	admin.AddCluster(cluster)
	log.Printf("added cluster: %s", cluster)
	//	defer admin.DropCluster(cluster)
	admin.AllowParticipantAutoJoin(cluster, true)

	// add 2 nodes to the cluster
	err := admin.AddNode(cluster, node1)
	must(err)
	err = admin.AddNode(cluster, node2)
	must(err)
	log.Printf("node: %s %s added to cluster[%s]", node1, node2, cluster)

	// define the resource and partition
	resourceOption := helix.DefaultAddResourceOption(partitions, stateModel)
	err = admin.AddResource(cluster, resource, resourceOption)
	must(err)
	log.Printf("resource[%s] partitions:%d model:%s added to cluster[%s]", resource,
		partitions, stateModel, cluster)

	// start contoller
	go func() {
		err = helix.StartController(cluster)
		must(err)
	}()
	log.Println("controller started")

	manager := zk.NewZKHelixManager(zkSvr)
	participant := manager.NewParticipant(cluster, "localhost", "10925")

	// creaet OnlineOffline state model
	sm := helix.NewStateModel([]helix.Transition{
		{"ONLINE", "OFFLINE", func(partition string) {
			log.Println("ONLINE-->OFFLINE")
		}},
		{"OFFLINE", "ONLINE", func(partition string) {
			log.Println("OFFLINE-->ONLINE")
		}},
	})

	participant.RegisterStateModel(stateModel, sm)

	err = participant.Start()
	must(err)
	log.Println("participant connected")

	instances, _ := admin.Instances(cluster)
	log.Printf("instances: %+v", instances)

	go func() {
		err = helix.Rebalance(cluster, resource, "2")
		must(err)
	}()
	log.Println("rebalanced")

	log.Println("waiting Ctrl-C...")

	// block until SIGINT and SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	return
}

func (*Demo) Synopsis() string {
	return "Demonstration how to use Helix"
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
