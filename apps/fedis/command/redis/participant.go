package redis

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/go-helix/store/zk"
	log "github.com/funkygao/log4go"
)

type redisParticipant struct {
	cluster, zkSvr       string
	resource, stateModel string
	replicas             int
	host, port           string

	instance       string
	instanceConfig *model.InstanceConfig

	m     helix.HelixManager
	redis *redislet
}

func NewNode(zkSvr, cluster, resource, stateModel string, replicas int, host, port string) *redisParticipant {
	return &redisParticipant{
		zkSvr:      zkSvr,
		cluster:    cluster,
		resource:   resource,
		stateModel: stateModel,
		replicas:   replicas,
		host:       host,
		port:       port,

		redis: newRedislet(port),
	}
}

func (r *redisParticipant) Start() {
	log.Info("starting redis %s:%s in cluster %s", r.host, r.port, r.cluster)

	mgr, _ := zk.NewZkParticipant(r.cluster, r.host, r.port, r.zkSvr,
		zk.WithZkSessionTimeout(time.Second*5), zk.WithPprofPort(10001))

	mgr.StateMachineEngine().RegisterStateModel(r.stateModel, r.StateModel())
	r.m = mgr

	must(mgr.Connect())
	log.Info("redis connected to cluster %s", r.cluster)

	r.instance = mgr.Instance()

	r.redis.SetManager(mgr)
	r.setupListener()

	r.instanceConfig, _ = mgr.ClusterManagementTool().InstanceConfig(r.cluster, r.instance)

	// TODO controller itself auto rebalance
	if false {
		log.Info("start rebalancing %s/%d ...", r.resource, r.replicas)
		admin := mgr.ClusterManagementTool()
		if err := admin.Rebalance(r.cluster, r.resource, r.replicas); err != nil {
			log.Error("rebalance: %v", err)
			return
		} else {
			log.Info("rebalance: ok")
		}
	}

	log.Info("awaiting Ctrl-C...")
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	mgr.Disconnect()
	log.Info("bye!")
	log.Close()
}
