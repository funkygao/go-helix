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
	cluster, zkSvr                 string
	resource, stateModel, replicas string
	host, port                     string
}

func NewNode(zkSvr, cluster, resource, stateModel, replicas, host, port string) *redisNode {
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
		helix.InstanceTypeParticipant, zk.WithManagerZkSessionTimeout(time.Second*10))
	manager.AddExternalViewChangeListener(func(externalViews []*model.Record, context *helix.Context) {
		log.Info(color.Red("%+v %+v", externalViews, context))
	})
	manager.AddIdealStateChangeListener(func(idealState []*model.Record, context *helix.Context) {
		log.Info(color.Yellow("%+v %+v", idealState, context))
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

	log.Info("start rebalancing %s/%s ...", r.replicas, r.replicas)
	helix.Rebalance(r.zkSvr, r.cluster, r.resource, r.replicas)
	log.Info("rebalanced done")

	log.Info("waiting Ctrl-C...")

	// block until SIGINT and SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}
