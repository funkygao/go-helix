package zk

import (
	"time"

	"github.com/funkygao/go-helix"
	log "github.com/funkygao/log4go"
)

type handlersMonitor struct {
	*Manager

	started bool
	stopper chan struct{}
	name    string
}

func newHandlersMonitor(m *Manager) helix.HelixTimerTask {
	return &handlersMonitor{
		name:    "handler monitor task",
		Manager: m,
	}
}

func (hm *handlersMonitor) Start() {
	log.Trace("start %s", hm.name)

	tick := time.NewTicker(time.Minute)
	defer tick.Stop()

	hm.stopper = make(chan struct{})
	hm.started = true

	for {
		select {
		case <-tick.C:
			// FIXME lock
			log.Info("%s handlers: %+v", hm.shortID(), hm.handlers)

		case <-hm.stopper:
			hm.started = false
			return
		}
	}

	return
}

func (hm *handlersMonitor) Stop() {
	log.Trace("stop %s", hm.name)

	if hm.started {
		close(hm.stopper)
	}
}
