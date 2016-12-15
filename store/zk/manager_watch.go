package zk

import (
	"time"

	"github.com/funkygao/go-helix"
	log "github.com/funkygao/log4go"
)

// startChangeNotificationLoop is the main event loop for Spectator.
// Whenever an external view update happpened
// the loop will pause for a short period of time to bucket all subsequent external view
// changes so that we don't send duplicate updates too often.
func (s *Manager) startChangeNotificationLoop() {
	log.Trace("%s starting change notification main loop...", s.shortID())

	if len(s.externalViewListeners) > 0 {
		s.watchExternalView()
	}

	if len(s.liveInstanceChangeListeners) > 0 {
		s.watchLiveInstances()
	}

	if len(s.currentStateChangeListeners) > 0 {
		s.watchCurrentStates()
	}

	if len(s.idealStateChangeListeners) > 0 {
		s.watchIdealState()
	}

	if len(s.controllerMessageListeners) > 0 {
		s.watchControllerMessages()
	}

	if len(s.instanceConfigChangeListeners) > 0 {
		s.watchInstanceConfig()
	}

	if len(s.messageListeners) > 0 {
		for instance := range s.messageListeners {
			s.watchInstanceMessages(instance)
		}
	}

	go func() {
		var err error
		for {
			select {
			case <-s.stop:
				return

			case err = <-s.changeNotificationErrChan:
				// e,g.
				// zk: session has been expired by the server
				log.Error("%s %v", s.shortID(), err)

			case chg := <-s.changeNotificationChan:
				s.handleChangeNotification(chg)
			}
		}
	}()

	log.Trace("%s started change notification main loop", s.shortID())
}

func (m *Manager) stopChangeNotificationLoop() {

}

func (s *Manager) handleChangeNotification(chg helix.ChangeNotification) {
	log.Debug("%s handle change notification: %s", s.shortID(),
		helix.ChangeNotificationText(chg.ChangeType))

	switch chg.ChangeType {
	case helix.ExternalViewChanged:
		if s.context != nil {
			s.context.Set("trigger", chg.ChangeData.(string))
		}

		externalViews := s.GetExternalView()
		for _, listener := range s.externalViewListeners {
			go listener(externalViews, s.context)
		}

	case helix.LiveInstanceChanged:
		liveInstances, _ := s.GetLiveInstances()
		for _, listener := range s.liveInstanceChangeListeners {
			go listener(liveInstances, s.context)
		}

	case helix.IdealStateChanged:
		idealStates := s.GetIdealState()
		for _, listener := range s.idealStateChangeListeners {
			go listener(idealStates, s.context)
		}

	case helix.CurrentStateChanged:
		instance := chg.ChangeData.(string)
		currentState := s.GetCurrentState(instance)
		for _, listener := range s.currentStateChangeListeners[instance] {
			go listener(instance, currentState, s.context)
		}

	case helix.InstanceConfigChanged:
		instanceConfigs := s.GetInstanceConfigs()
		for _, listener := range s.instanceConfigChangeListeners {
			go listener(instanceConfigs, s.context)
		}

	case helix.ControllerMessagesChanged:
		controllerMessages, _ := s.GetControllerMessages()
		for _, listener := range s.controllerMessageListeners {
			go listener(controllerMessages, s.context)
		}

	case helix.InstanceMessagesChanged:
		instance := chg.ChangeData.(string)
		messageRecords, _ := s.GetInstanceMessages(instance)
		for _, listener := range s.messageListeners[instance] {
			go listener(instance, messageRecords, s.context)
		}
	}
}

func (s *Manager) watchCurrentStates() {
	for instance := range s.currentStateChangeListeners {
		s.watchCurrentStateForInstance(instance)
	}
}

func (s *Manager) watchCurrentStateForInstance(instance string) {
	sessions, err := s.conn.Children(s.kb.currentStates(instance))
	if err != nil {
		s.changeNotificationErrChan <- err
		return
	}

	// TODO: only have one session?
	if len(sessions) > 0 {
		resources, err := s.conn.Children(s.kb.currentStatesForSession(instance, sessions[0]))
		if err != nil {
			s.changeNotificationErrChan <- err
			return
		}

		for _, resource := range resources {
			s.watchCurrentStateOfInstanceForResource(instance, resource, sessions[0])
		}
	}
}

func (s *Manager) watchCurrentStateOfInstanceForResource(instance string, resource string, sessionID string) {
	watchPath := s.kb.currentStateForResource(instance, sessionID, resource)
	if _, ok := s.stopCurrentStateWatch[watchPath]; !ok {
		s.stopCurrentStateWatch[watchPath] = make(chan interface{})
	}

	// check if the session are ever expired. If so, remove the watcher
	go func() {
		c := time.Tick(10 * time.Second)
		for now := range c {
			if ok, err := s.conn.Exists(watchPath); !ok || err != nil {
				s.stopCurrentStateWatch[watchPath] <- now
				return
			}
		}
	}()

	go func() {
		for {
			_, events, err := s.conn.GetW(watchPath)
			if err != nil {
				s.changeNotificationErrChan <- err
				return
			}

			select {
			case <-events:
				s.changeNotificationChan <- helix.ChangeNotification{
					ChangeType: helix.CurrentStateChanged,
					ChangeData: instance,
				}

			case <-s.stopCurrentStateWatch[watchPath]:
				delete(s.stopCurrentStateWatch, watchPath)
				return
			}
		}
	}()
}

func (s *Manager) watchLiveInstances() {
	go func() {
		for {
			_, events, err := s.conn.ChildrenW(s.kb.liveInstances())
			if err != nil {
				s.changeNotificationErrChan <- err
				return
			}

			// notify the live instance update
			s.changeNotificationChan <- helix.ChangeNotification{
				ChangeType: helix.LiveInstanceChanged,
				ChangeData: nil,
			}

			// block the loop to wait for the live instance change
			evt := <-events
			if evt.Err != nil {
				s.changeNotificationErrChan <- evt.Err
				return
			}
		}
	}()
}

func (s *Manager) watchInstanceConfig() {
	go func() {
		for {
			configs, events, err := s.conn.ChildrenW(s.kb.participantConfigs())
			if err != nil {
				s.changeNotificationErrChan <- err
				return
			}

			// find the resources that are newly added, and create a watcher
			for _, k := range configs {
				if _, present := s.instanceConfigMap[k]; !present {
					s.watchInstanceConfigForParticipant(k)

					s.instanceConfigMap[k] = true
				}
			}

			// refresh the instanceConfigMap to make sure only the currently existing resources
			// are marked as true
			for k := range s.instanceConfigMap {
				s.instanceConfigMap[k] = false
			}
			for _, k := range configs {
				s.instanceConfigMap[k] = true
			}

			// Notify an update of external view if there are new resources added.
			s.changeNotificationChan <- helix.ChangeNotification{
				ChangeType: helix.InstanceConfigChanged,
				ChangeData: nil,
			}

			// now need to block the loop to wait for the next update event
			evt := <-events
			if evt.Err != nil {
				s.changeNotificationErrChan <- err
				return
			}
		}
	}()
}

func (s *Manager) watchInstanceConfigForParticipant(instance string) {
	go func() {
		for {
			// block and wait for the next update for the resource
			// when the update happens, unblock, and also send the resource
			// to the channel
			_, events, err := s.conn.GetW(s.kb.participantConfig(instance))
			if err != nil {
				s.changeNotificationErrChan <- err
				return
			}

			<-events
			s.changeNotificationChan <- helix.ChangeNotification{
				ChangeType: helix.InstanceConfigChanged,
				ChangeData: instance,
			}
		}
	}()

}

func (s *Manager) watchIdealState() {
	go func() {
		for {
			resources, events, err := s.conn.ChildrenW(s.kb.idealStates())
			if err != nil {
				s.changeNotificationErrChan <- err
				return
			}

			// find the resources that are newly added, and create a watcher
			for _, resource := range resources {
				if _, present := s.idealStateResourceMap[resource]; !present {
					s.watchIdealStateResource(resource)
					s.idealStateResourceMap[resource] = true
				}
			}

			// refresh the idealStateResourceMap to make sure only the currently existing resources
			// are marked as true
			for resource := range s.idealStateResourceMap {
				s.idealStateResourceMap[resource] = false
			}
			for _, resource := range resources {
				s.idealStateResourceMap[resource] = true
			}

			// Notify an update of external view if there are new resources added.
			s.changeNotificationChan <- helix.ChangeNotification{
				ChangeType: helix.IdealStateChanged,
				ChangeData: nil,
			}

			// now need to block the loop to wait for the next update event
			evt := <-events
			if evt.Err != nil {
				s.changeNotificationErrChan <- evt.Err
			}
		}
	}()
}

func (s *Manager) watchIdealStateResource(resource string) {
	go func() {
		for {
			// block and wait for the next update for the resource
			// when the update happens, unblock, and also send the resource
			// to the channel
			_, events, err := s.conn.GetW(s.kb.idealStateForResource(resource))
			if err != nil {
				s.changeNotificationErrChan <- err
				return
			}

			<-events
			s.changeNotificationChan <- helix.ChangeNotification{
				ChangeType: helix.IdealStateChanged,
				ChangeData: resource,
			}
		}
	}()
}

func (s *Manager) watchExternalView() {
	go func() {
		for {
			resources, events, err := s.conn.ChildrenW(s.kb.externalView())
			if err != nil {
				s.changeNotificationErrChan <- err
				return
			}

			// find the resources that are newly added, and create a watcher
			// FIXME thread safety
			for _, resource := range resources {
				if _, present := s.externalViewResourceMap[resource]; !present {
					s.watchExternalViewResource(resource)
					s.externalViewResourceMap[resource] = true
				}
			}

			// refresh the externalViewResourceMap to make sure only the currently existing resources
			// are marked as true
			for resource := range s.externalViewResourceMap {
				s.externalViewResourceMap[resource] = false
			}
			for _, resource := range resources {
				s.externalViewResourceMap[resource] = true
			}

			// Notify an update of external view if there are new resources added.
			s.changeNotificationChan <- helix.ChangeNotification{
				ChangeType: helix.ExternalViewChanged,
				ChangeData: "",
			}

			// now need to block the loop to wait for the next update event
			evt := <-events
			if evt.Err != nil {
				s.changeNotificationErrChan <- evt.Err
			}
		}
	}()
}

func (s *Manager) watchExternalViewResource(resource string) {
	go func() {
		for {
			_, events, err := s.conn.GetW(s.kb.externalViewForResource(resource))
			if err != nil {
				s.changeNotificationErrChan <- err
			}

			<-events
			s.changeNotificationChan <- helix.ChangeNotification{
				ChangeType: helix.ExternalViewChanged,
				ChangeData: resource,
			}
		}
	}()
}

// watchControllerMessages only watch the changes of message list, it currently
// doesn't watch the content of the messages.
func (s *Manager) watchControllerMessages() {
	go func() {
		_, events, err := s.conn.ChildrenW(s.kb.controllerMessages())
		if err != nil {
			panic(err)
		}

		// send the INIT update
		s.changeNotificationChan <- helix.ChangeNotification{
			ChangeType: helix.ControllerMessagesChanged,
			ChangeData: nil,
		}

		// block to wait for CALLBACK
		<-events
	}()
}

func (s *Manager) watchInstanceMessages(instance string) {
	go func() {
		messages, events, err := s.conn.ChildrenW(s.kb.messages(instance))
		if err != nil {
			panic(err)
		}

		for _, m := range messages {
			s.receivedMessages.Add(m, nil)
		}

		s.instanceMessageChannel <- instance

		// block and wait for next change
		<-events
	}()
}

// watchInstanceMessage will watch an individual message and trigger update
// if the content of the message has changed.
func (s *Manager) watchInstanceMessage(instance string, messageID string) {
	go func() {

	}()
}
