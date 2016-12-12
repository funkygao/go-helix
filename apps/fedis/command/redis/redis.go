package redis

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/go-helix/store/zk"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
)

type redisNode struct {
	cluster, zkSvr       string
	resource, stateModel string
	replicas             int
	host, port           string
}

func NewNode(zkSvr, cluster, resource, stateModel string, replicas int, host, port string) *redisNode {
	return &redisNode{
		zkSvr:      zkSvr,
		cluster:    cluster,
		resource:   resource,
		stateModel: stateModel,
		replicas:   replicas,
		host:       host,
		port:       port,
	}
}

func (r *redisNode) Start() {
	// create the manager instance and connect
	manager, _ := zk.NewZkHelixManager(r.cluster, r.host, r.port, r.zkSvr,
		helix.InstanceTypeParticipant,
		zk.WithManagerZkSessionTimeout(time.Second*5),
		zk.WithPprofPort(10001))

	manager.AddPreConnectCallback(func() {
		log.Info("will be connecting...")
	})

	manager.AddExternalViewChangeListener(func(externalViews []*model.Record, context *helix.Context) {
		log.Info(color.Red("external view changed: %+v %+v", externalViews, context))
	})
	manager.AddIdealStateChangeListener(func(idealState []*model.Record, context *helix.Context) {
		log.Info(color.Yellow("ideal state changed: %+v %+v", idealState, context))
	})

	// register state model before connecting
	sm := helix.NewStateModel()
	must(sm.AddTransitions([]helix.Transition{
		{"MASTER", "SLAVE", func(message *model.Message, context *helix.Context) {
			log.Info(color.Green("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},

		{"SLAVE", "MASTER", func(message *model.Message, context *helix.Context) {
			log.Info(color.Cyan("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},

		{"OFFLINE", "SLAVE", func(message *model.Message, context *helix.Context) {
			log.Info(color.Blue("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},

		{"SLAVE", "OFFLINE", func(message *model.Message, context *helix.Context) {
			log.Info(color.Red("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},
	}))
	manager.StateMachineEngine().RegisterStateModel(r.stateModel, sm)

	must(manager.Connect())

	log.Info("start rebalancing %s/%d ...", r.resource, r.replicas)
	admin := manager.ClusterManagementTool()
	if err := admin.Rebalance(r.cluster, r.resource, r.replicas); err != nil {
		log.Error("rebalance: %v", err)
	} else {
		log.Info("rebalance: ok")
	}

	log.Info("waiting Ctrl-C...")

	// block until SIGINT and SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}
