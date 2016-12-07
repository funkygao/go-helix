package zk

import (
	log "github.com/funkygao/log4go"
	"github.com/yichen/go-zookeeper/zk"
)

func (m *Manager) zkStateHandler(state zk.State) {
	log.Debug("%s", state)

	switch state {
	case zk.StateDisconnected:
	case zk.StateExpired:
	case zk.StateHasSession:
	}
}
