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

	prevMaster string
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
	// create the redisInstance instance and connect
	redisInstance, _ := zk.NewZkParticipant(r.cluster, r.host, r.port, r.zkSvr,
		zk.WithZkSessionTimeout(time.Second*5),
		zk.WithPprofPort(10001))

	redisInstance.AddPreConnectCallback(func() {
		log.Info("Will be connecting...")
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

			// catch up previous master, enable writes, etc.
		}},

		{"OFFLINE", "SLAVE", func(message *model.Message, context *helix.Context) {
			log.Info(color.Blue("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))

			r.prevMaster = "" // get from current state
			// bootstrap data, setup replication, etc.
		}},

		{"SLAVE", "OFFLINE", func(message *model.Message, context *helix.Context) {
			log.Info(color.Red("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},
	}))
	redisInstance.StateMachineEngine().RegisterStateModel(r.stateModel, sm)

	must(redisInstance.Connect())

	if err := redisInstance.AddExternalViewChangeListener(
		func(externalViews []*model.ExternalView, context *helix.Context) {
			log.Info(color.Red("external view changed: %+v %+v", externalViews, context))
		}); err != nil {
		log.Error("%s", err)
	}
	if err := redisInstance.AddIdealStateChangeListener(
		func(idealState []*model.IdealState, context *helix.Context) {
			log.Info(color.Yellow("ideal state changed: %+v %+v", idealState, context))
		}); err != nil {
		log.Error("%s", err)
	}
	if err := redisInstance.AddLiveInstanceChangeListener(
		func(liveInstances []*model.LiveInstance, context *helix.Context) {
			log.Info("live instances: %+v %v", liveInstances, context)
		}); err != nil {
		log.Error("%s", err)
	}
	if err := redisInstance.AddCurrentStateChangeListener(redisInstance.Instance(), redisInstance.SessionID(),
		func(instance string, currentState []*model.CurrentState, context *helix.Context) {
			log.Info("current state[%s] %+v", instance, currentState)
		}); err != nil {
		log.Error("%s", err)
	}

	log.Info("start rebalancing %s/%d ...", r.resource, r.replicas)
	admin := redisInstance.ClusterManagementTool()
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
