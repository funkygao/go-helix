package redis

import (
	"os"
	"os/exec"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/redigo/redis"
)

// redislet is a single redis instance that can be slave or master.
// redislet automatically replicate from master if it is slave.
// TODO sync up to date with master
type redislet struct {
	ctx *redisParticipant

	msg *model.Message

	port string
	rc   redis.Conn

	master *model.InstanceConfig
}

func newRedislet(port string) *redislet {
	r := &redislet{
		port: port,
	}
	r.bootRedis()

	time.Sleep(time.Second)
	rc, err := redis.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		panic(err)
	}

	r.rc = rc
	return r
}

func (r *redislet) TopologyAware(externalViews []*model.ExternalView, ctx *helix.Context) {
	log.Info(color.Yellow("%+v", externalViews))

	r.locateMaster(externalViews)
}

func (r *redislet) StartMaster() {
	log.Info("start redis master and accept RW: %s", r.msg.PartitionName())

	// bootstrap redis as master
	// now, I don't care who is master
	r.callRedis("READWRITE")
}

func (r *redislet) StopMaster() {
	log.Info("redis master stopped")
}

func (r *redislet) locateMaster(externalViews []*model.ExternalView) {
	if r.msg == nil {
		return
	}

	for _, e := range externalViews {
		if e.Resource() != r.msg.Resource() {
			continue
		}

		instance := e.InstanceWithState(r.msg.PartitionName(), "MASTER")
		if instance == "" {
			log.Warn("no master found !!!")
			return
		}

		cf, err := r.ctx.m.ClusterManagementTool().InstanceConfig(r.ctx.cluster, instance)
		if err != nil {
			panic(err)
		}

		if r.master != cf {
			log.Info("FOUND NEW master: %+v", cf)
			// TODO stop current replication and start new replication
		}
		r.master = cf
	}
}

func (r *redislet) StartReplication() {
	r.callRedis("READONLY")

	log.Trace("start replication from master %+v", r.master)

	if r.master == nil {
		log.Warn("no master found")
		return
	}

	log.Info("slave bootstrap data and start replication from master %s", r.master.Node())
	r.callRedis("SLAVEOF", r.master.Host(), r.master.Port())

}

func (r *redislet) StopReplication() {
	log.Info("slave stop replication")
	r.callRedis("SLAVEOF", "NO", "ONE")
}

func (r *redislet) SetContext(p *redisParticipant) {
	r.ctx = p

	log.Trace("instance config: %+v", p.instanceConfig)
}

func (r *redislet) bootRedis() {
	log.Info("killing running redis")
	cmd := exec.Command("pkill", "redis")
	cmd.Start()
	cmd.Wait()

	log.Info("starting new redis on :%s", r.port)
	waitStartCh := make(chan struct{})
	cmd = exec.Command(os.Getenv("REDIS"), "--port", r.port)
	go func() {
		<-waitStartCh

		log.Info("redis[%s] started", os.Getenv("REDIS"))
		if err := cmd.Wait(); err != nil {
			panic(err)
		}
	}()

	if err := cmd.Start(); err != nil {
		panic(err)
	}

	waitStartCh <- struct{}{}
}

func (r *redislet) callRedis(cmd string, args ...interface{}) {
	reply, err := r.rc.Do(cmd, args...)
	if err != nil {
		//panic(err)
		log.Error("call redis: %s %+v -> %v", cmd, args, err)
		return
	}

	log.Debug("call redis: %s %+v -> %+v", cmd, args, reply)
}
