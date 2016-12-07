package zk

import (
	"github.com/funkygao/go-helix"
	log "github.com/funkygao/log4go"
	"github.com/yichen/go-zookeeper/zk"
)

var _ ZkStateListener = &Manager{}

func (m *Manager) HandleStateChanged(state zk.State) error {
	log.Debug("new state: %s", state)

	switch state {
	case zk.StateSyncConnected:

	case zk.StateDisconnected:

	case zk.StateExpired:
	}
	return nil
}

func (m *Manager) HandleNewSession() (err error) {
	if err = m.conn.waitUntilConnected(); err != nil {
		return
	}

	switch m.it {
	case helix.InstanceTypeParticipant:
		err = m.handleNewSessionAsParticipant()

	case helix.InstanceTypeController:
		err = m.handleNewSessionAsController()

	case helix.InstanceTypeSpectator:

	case helix.InstanceTypeNotImplemented:
		return helix.ErrNotImplemented
	}

	// TODO restart all listeners, re-watch, recreate ephemeral znodes
	m.startChangeNotificationLoop()

	return
}

func (m *Manager) handleNewSessionAsParticipant() error {
	p := newParticipant(m)

	if ok, err := p.joinCluster(); !ok || err != nil {
		if err != nil {
			return err
		}
		return helix.ErrEnsureParticipantConfig
	}

	// invoke preconnection callbacks
	for _, cb := range m.preConnectCallbacks {
		cb()
	}

	if err := p.createLiveInstance(); err != nil {
		return err
	}

	if err := p.carryOverPreviousCurrentState(); err != nil {
		return err
	}

	p.setupMsgHandler()

	return nil
}

func (m *Manager) handleNewSessionAsController() error {
	return helix.ErrNotImplemented
}
