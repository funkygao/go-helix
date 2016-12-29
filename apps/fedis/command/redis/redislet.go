package redis

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
)

// redislet is a single redis instance that can be slave or master.
// redislet automatically replicate from master if it is slave.
type redislet struct {
	m helix.HelixManager

	port   string
	master string // host:port of master if this redis is slave
}

func newRedislet(port string) *redislet {
	return &redislet{
		port: port,
	}
}

func (r *redislet) SetMaster(master string) {
	r.master = master
}

func (r *redislet) SetManager(m helix.HelixManager) {
	r.m = m
}

func (r *redislet) Replicator(externalViews []*model.ExternalView, ctx *helix.Context) {
	log.Info(color.Cyan("%+v", externalViews))
}
