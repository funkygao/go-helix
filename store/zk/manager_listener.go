package zk

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-zookeeper/zk"
	log "github.com/funkygao/log4go"
)

func (m *Manager) initHandlers() {
	log.Trace("%s init handlers", m.shortID())

	m.Lock()
	defer m.Unlock()

	for _, handler := range m.handlers {
		handler.Init()
	}
}

func (m *Manager) resetHandlers() {
	log.Trace("%s reset handlers", m.shortID())

	m.Lock()
	defer m.Unlock()

	for _, handler := range m.handlers {
		handler.Reset()
	}
}

func (m *Manager) handleListenerErrors() {
	defer m.wg.Done()

	for {
		select {
		case <-m.stop:
			return

		case err, ok := <-m.conn.LisenterErrors():
			log.Error("%v %v", ok, err) // TODO err should contain path
		}
	}
}

func (m *Manager) addListener(listener interface{}, path string,
	changeType helix.ChangeNotificationType, events []zk.EventType) error {
	if !m.IsConnected() {
		return helix.ErrNotConnected
	}

	m.Lock()
	defer m.Unlock()

	for _, handler := range m.handlers {
		if handler.listener == listener && handler.path == path {
			return helix.ErrDupOperation
		}
	}

	cb := newCallbackHandler(m, path, listener, changeType, events)
	m.handlers = append(m.handlers, cb)
	return nil
}

func (m *Manager) AddExternalViewChangeListener(listener helix.ExternalViewChangeListener) error {
	return m.addListener(listener, m.kb.externalView(), helix.ExternalViewChanged,
		[]zk.EventType{
			zk.EventNodeChildrenChanged,
			zk.EventNodeDeleted, // TODO
			zk.EventNodeCreated, // TODO
		})
}

func (m *Manager) AddLiveInstanceChangeListener(listener helix.LiveInstanceChangeListener) error {
	return m.addListener(listener, m.kb.liveInstances(), helix.LiveInstanceChanged,
		[]zk.EventType{
			zk.EventNodeDataChanged,
			zk.EventNodeChildrenChanged,
			zk.EventNodeDeleted,
			zk.EventNodeCreated,
		})
}

func (m *Manager) AddCurrentStateChangeListener(instance string, sessionID string, listener helix.CurrentStateChangeListener) error {
	return m.addListener(listener, m.kb.currentStatesForSession(instance, sessionID), helix.CurrentStateChanged,
		[]zk.EventType{
			zk.EventNodeChildrenChanged,
			zk.EventNodeDeleted,
			zk.EventNodeCreated,
		})
}

// TODO Decide if do we still need this since we are exposing ClusterMessagingService
func (m *Manager) AddMessageListener(instance string, listener helix.MessageListener) error {
	return m.addListener(listener, m.kb.messages(instance), helix.InstanceMessagesChanged,
		[]zk.EventType{
			zk.EventNodeChildrenChanged,
			zk.EventNodeDeleted,
			zk.EventNodeCreated,
		})
}

func (m *Manager) AddIdealStateChangeListener(listener helix.IdealStateChangeListener) error {
	return m.addListener(listener, m.kb.idealStates(), helix.IdealStateChanged,
		[]zk.EventType{
			zk.EventNodeDataChanged,
			zk.EventNodeDeleted,
			zk.EventNodeCreated,
		})
}

func (m *Manager) AddInstanceConfigChangeListener(listener helix.InstanceConfigChangeListener) error {
	return m.addListener(listener, m.kb.instances(), helix.InstanceConfigChanged,
		[]zk.EventType{
			zk.EventNodeChildrenChanged,
		})
}

func (m *Manager) AddControllerMessageListener(listener helix.ControllerMessageListener) error {
	return m.addListener(listener, m.kb.controllerMessages(), helix.ControllerMessagesChanged,
		[]zk.EventType{
			zk.EventNodeChildrenChanged,
			zk.EventNodeDeleted,
			zk.EventNodeCreated,
		})
}

func (m *Manager) AddControllerListener(listener helix.ControllerChangeListener) error {
	return m.addListener(listener, m.kb.controller(), helix.ControllerChanged,
		[]zk.EventType{
			zk.EventNodeChildrenChanged,
			zk.EventNodeDeleted,
			zk.EventNodeCreated,
		})
}

func (m *Manager) RemoveListener(path string, listener interface{}) error {
	m.Lock()
	defer m.Unlock()

	newHandlers := make([]*CallbackHandler, 0, len(m.handlers))
	var toRemove *CallbackHandler
	for _, handler := range m.handlers {
		if handler.path == path && handler.listener == listener {
			toRemove = handler
			break
		}

		newHandlers = append(newHandlers, handler)
	}

	m.handlers = newHandlers

	if toRemove != nil {
		toRemove.Reset()
	}

	return helix.ErrNotImplemented
}
