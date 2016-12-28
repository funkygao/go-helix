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

func (r *redisNode) StateModel() *helix.StateModel {
	// register state model before connecting
	sm := helix.NewStateModel()
	must(sm.AddTransitions([]helix.Transition{
		{"MASTER", "SLAVE", func(message *model.Message, ctx *helix.Context) {
			log.Info(color.Cyan("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},

		{"SLAVE", "MASTER", func(message *model.Message, ctx *helix.Context) {
			log.Info(color.Green("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))

			// catch up previous master, enable writes, etc.
		}},

		{"OFFLINE", "SLAVE", func(message *model.Message, ctx *helix.Context) {
			log.Info(color.Blue("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))

			r.prevMaster = "" // get from current state
			// bootstrap data, setup replication, etc.
		}},

		{"SLAVE", "OFFLINE", func(message *model.Message, ctx *helix.Context) {
			log.Info(color.Red("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},
	}))

	return sm
}

func (r *redisNode) Start() {
	log.Info("starting redis %s:%s in cluster %s", r.host, r.port, r.cluster)

	// create the redisInstance instance and connect
	redisInstance, _ := zk.NewZkParticipant(r.cluster, r.host, r.port, r.zkSvr,
		zk.WithZkSessionTimeout(time.Second*5),
		zk.WithPprofPort(10001))

	redisInstance.AddPreConnectCallback(func() {
		log.Info("Connecting to cluster...")
	})
	redisInstance.AddPostConnectCallback(func() {
		log.Info("Connected to cluster")
	})

	redisInstance.StateMachineEngine().RegisterStateModel(r.stateModel, r.StateModel())

	must(redisInstance.Connect())

	log.Info("redis connected to cluster %s", r.cluster)

	log.Info("tracing current state change")
	if err := redisInstance.AddCurrentStateChangeListener(redisInstance.Instance(), redisInstance.SessionID(),
		func(instance string, currentState []*model.CurrentState, ctx *helix.Context) {
			log.Info("current state[%s] %+v", instance, currentState)
		}); err != nil {
		log.Error("%s", err)
	}

	log.Info("start rebalancing %s/%d ...", r.resource, r.replicas)
	admin := redisInstance.ClusterManagementTool()
	if err := admin.Rebalance(r.cluster, r.resource, r.replicas); err != nil {
		log.Error("rebalance: %v", err)
		return
	} else {
		log.Info("rebalance: ok")
	}

	log.Info("waiting Ctrl-C...")

	// block until SIGINT and SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	redisInstance.Disconnect()
	log.Info("bye!")
	log.Close()
}
