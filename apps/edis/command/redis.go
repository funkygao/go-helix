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
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/go-helix/store/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
)

type Redis struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Redis) Run(args []string) (exitCode int) {
	var node string
	cmdFlags := flag.NewFlagSet("redis", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&node, "node", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	tuples := strings.Split(node, "_")
	if len(tuples) != 2 {
		this.Ui.Error("node must in form of host_port")
		return 2
	}

	host := tuples[0]
	port := tuples[1]

	// create the manager instance and connect
	manager := zk.NewZKHelixManager(zkSvr, zk.WithSessionTimeout(time.Second*10))
	must(manager.Connect())

	// the actual task executor
	participant := manager.NewParticipant(cluster, host, port)
	sm := helix.NewStateModel()
	sm.AddTransitions([]helix.Transition{
		{"OFFLINE", "SLAVE", func(message *model.Message, context *helix.Context) {
			log.Info(color.Green("resource[%s] partition[%s] OFFLINE-->SLAVE",
				message.Resource(),
				message.PartitionName()))
		}},

		{"SLAVE", "MASTER", func(message *model.Message, context *helix.Context) {
			log.Info(color.Cyan("resource[%s] partition[%s] SLAVE-->MASTER",
				message.Resource(),
				message.PartitionName()))
		}},
	})
	participant.RegisterStateModel(stateModel, sm)

	must(participant.Start())
	log.Info("participant started")

	log.Info("start rebalancing...")
	helix.Rebalance(zkSvr, cluster, resource, replicas)
	log.Info("rebalanced done")

	log.Info("waiting Ctrl-C...")

	// block until SIGINT and SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	return
}

func (*Redis) Synopsis() string {
	return "Start a redis instance"
}

func (this *Redis) Help() string {
	help := fmt.Sprintf(`
Usage: %s redis [options]

    %s

Options:

    -node host_port

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
