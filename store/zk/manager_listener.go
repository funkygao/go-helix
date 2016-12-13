package zk

import (
	"github.com/funkygao/go-helix"
)

func (s *Manager) AddExternalViewChangeListener(listener helix.ExternalViewChangeListener) {
	s.Lock()
	s.externalViewListeners = append(s.externalViewListeners, listener)
	s.Unlock()
}

func (s *Manager) AddLiveInstanceChangeListener(listener helix.LiveInstanceChangeListener) {
	s.Lock()
	s.liveInstanceChangeListeners = append(s.liveInstanceChangeListeners, listener)
	s.Unlock()
}

// AddCurrentStateChangeListener.
// FIXME sessionID
func (s *Manager) AddCurrentStateChangeListener(instance string, sessionID string, listener helix.CurrentStateChangeListener) {
	s.Lock()
	defer s.Unlock()

	if s.currentStateChangeListeners[instance] == nil {
		s.currentStateChangeListeners[instance] = []helix.CurrentStateChangeListener{}
	}

	s.currentStateChangeListeners[instance] = append(s.currentStateChangeListeners[instance], listener)

	// if we are adding new listeners when the specator is already connected, we need
	// to kick of the listener in the event loop
	if len(s.currentStateChangeListeners[instance]) == 1 && s.IsConnected() {
		s.watchCurrentStateForInstance(instance)
	}
}

func (s *Manager) AddMessageListener(instance string, listener helix.MessageListener) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.messageListeners[instance]; !ok {
		s.messageListeners[instance] = []helix.MessageListener{}
	}

	s.messageListeners[instance] = append(s.messageListeners[instance], listener)

	// if the spectator is already connected and this is the first listener
	// for the instance, we need to start watching the zookeeper path for
	// upcoming messages
	if len(s.messageListeners[instance]) == 1 && s.IsConnected() {
		s.watchInstanceMessages(instance)
	}
}

func (s *Manager) AddIdealStateChangeListener(listener helix.IdealStateChangeListener) {
	s.Lock()
	s.idealStateChangeListeners = append(s.idealStateChangeListeners, listener)
	s.Unlock()
}

func (s *Manager) AddInstanceConfigChangeListener(listener helix.InstanceConfigChangeListener) {
	s.Lock()
	s.instanceConfigChangeListeners = append(s.instanceConfigChangeListeners, listener)
	s.Unlock()
}

func (s *Manager) AddControllerMessageListener(listener helix.ControllerMessageListener) {
	s.Lock()
	s.controllerMessageListeners = append(s.controllerMessageListeners, listener)
	s.Unlock()
}

func (s *Manager) RemoveListener() {
	panic(helix.ErrNotImplemented)
}
