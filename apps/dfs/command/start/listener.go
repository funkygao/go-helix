package start

import (
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
)

func (r *redisParticipant) setupListener() {
	log.Info("tracing current state change")

	go func() {
		// race condition with controller setup my current state
		// too early, the znode not exist
		time.Sleep(time.Second * 10)

		mgr := r.m
		if err := mgr.AddCurrentStateChangeListener(mgr.Instance(), mgr.SessionID(), r.currentStateListener); err != nil {
			panic(err)
		}
	}()

}

func (r *redisParticipant) currentStateListener(instance string, currentState []*model.CurrentState, ctx *helix.Context) {
	log.Info(color.Colorize([]string{color.Blink, color.FgYellow}, "current state[%s] %+v", instance, currentState))
}
