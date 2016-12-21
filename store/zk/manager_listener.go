package zk

import (
	"github.com/funkygao/go-helix"
	log "github.com/funkygao/log4go"
)

func (m *Manager) initHandlers() {
	log.Trace("%s init handlers", m.shortID())

	m.Lock()
	defer m.Unlock()

	for _, handler := range m.handlers {
		log.Debug("%s init %s", m.shortID(), handler)
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

	log.Trace("%s start listener errors handler", m.shortID())

	for {
		select {
		case <-m.stop:
			return

		case err, ok := <-m.conn.LisenterErrors():
			if !ok {
				log.Warn("%s listener error channel closed", m.shortID())
				return
			}

			log.Error("%s %s %s", m.shortID(), err.Path, err.Error())
		}
	}
}

func (m *Manager) addListener(listener interface{}, path string, changeType helix.ChangeNotificationType, watchChild bool) error {
	if !m.IsConnected() {
		return helix.ErrNotConnected
	}

	log.Debug("%s add listener %s for %s", m.shortID(), changeType, path)

	m.Lock()
	defer m.Unlock()

	for _, handler := range m.handlers {
		if handler.listener == listener && handler.path == path {
			return helix.ErrDupOperation
		}
	}

	cb := newCallbackHandler(m, path, listener, changeType, watchChild)
	m.handlers = append(m.handlers, cb)
	return nil
}

func (m *Manager) AddExternalViewChangeListener(listener helix.ExternalViewChangeListener) error {
	return m.addListener(listener, m.kb.externalView(), helix.ExternalViewChanged, true)
}

func (m *Manager) AddLiveInstanceChangeListener(listener helix.LiveInstanceChangeListener) error {
	return m.addListener(listener, m.kb.liveInstances(), helix.LiveInstanceChanged, true)
}

func (m *Manager) AddCurrentStateChangeListener(instance string, sessionID string, listener helix.CurrentStateChangeListener) error {
	return m.addListener(listener, m.kb.currentStatesForSession(instance, sessionID), helix.CurrentStateChanged, true)
}

func (m *Manager) AddMessageListener(instance string, listener helix.MessageListener) error {
	return m.addListener(listener, m.kb.messages(instance), helix.InstanceMessagesChanged, false)
}

func (m *Manager) AddIdealStateChangeListener(listener helix.IdealStateChangeListener) error {
	return m.addListener(listener, m.kb.idealStates(), helix.IdealStateChanged, true)
}

func (m *Manager) AddInstanceConfigChangeListener(listener helix.InstanceConfigChangeListener) error {
	return m.addListener(listener, m.kb.participantConfigs(), helix.InstanceConfigChanged, true)
}

func (m *Manager) AddControllerMessageListener(listener helix.MessageListener) error {
	return m.addListener(listener, m.kb.controllerMessages(), helix.ControllerMessagesChanged, false)
}

func (m *Manager) AddControllerListener(listener helix.ControllerChangeListener) error {
	return m.addListener(listener, m.kb.controller(), helix.ControllerChanged, false)
}

func (m *Manager) RemoveListener(path string, listener interface{}) error {
	log.Debug("%s remove listener %s %#v", m.shortID(), path, listener)

	m.Lock()
	defer m.Unlock()

	newHandlers := make([]*CallbackHandler, 0, len(m.handlers))
	var toRemove *CallbackHandler
	for _, handler := range m.handlers {
		if handler.path == path && handler.listener == listener {
			toRemove = handler
		} else {
			newHandlers = append(newHandlers, handler)
		}
	}

	m.handlers = newHandlers

	if toRemove != nil {
		toRemove.Reset()
	}

	return nil
}
