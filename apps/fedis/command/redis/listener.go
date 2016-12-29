package redis

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
)

func (r *redisParticipant) setupListener() {
	log.Info("tracing current state change")

	mgr := r.m
	if err := mgr.AddCurrentStateChangeListener(mgr.Instance(), mgr.SessionID(), r.currentStateListener); err != nil {
		panic(err)
	}
}

func (r *redisParticipant) currentStateListener(instance string, currentState []*model.CurrentState, ctx *helix.Context) {
	log.Info("current state[%s] %+v", instance, currentState)
}
