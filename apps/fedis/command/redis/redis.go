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
	manager := zk.NewZKHelixManager(r.zkSvr, zk.WithSessionTimeout(time.Second*10))
	must(manager.Connect())

	// the actual task executor
	participant := manager.NewParticipant(r.cluster, r.host, r.port)

	// register state model
	sm := helix.NewStateModel()
	sm.AddTransitions([]helix.Transition{
		{"MASTER", "SLAVE", func(message *model.Message, context *helix.Context) {
			log.Info(color.Green("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},

		{"SLAVE", "MASTER", func(message *model.Message, context *helix.Context) {
			log.Info(color.Green("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},

		{"OFFLINE", "SLAVE", func(message *model.Message, context *helix.Context) {
			log.Info(color.Green("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},

		{"SLAVE", "OFFLINE", func(message *model.Message, context *helix.Context) {
			log.Info(color.Green("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},

		{"OFFLINE", "DROPPED", func(message *model.Message, context *helix.Context) {
			log.Info(color.Green("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},
	})
	participant.RegisterStateModel(r.stateModel, sm)

	// start the participant
	must(participant.Start())
	log.Info("participant started")

	log.Info("start rebalancing...")
	helix.Rebalance(r.zkSvr, r.cluster, r.resource, r.replicas)
	log.Info("rebalanced done")

	log.Info("waiting Ctrl-C...")

	// block until SIGINT and SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}