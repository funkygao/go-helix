package zk

import (
	"fmt"
	"time"

	"github.com/funkygao/go-helix"
)

func (s *Manager) watchCurrentStates() {
	for k := range s.currentStateChangeListeners {
		s.watchCurrentStateForInstance(k)
	}
}

func (s *Manager) watchCurrentStateForInstance(instance string) {
	sessions, err := s.conn.Children(s.kb.currentStates(instance))
	must(err)

	// TODO: only have one session?
	if len(sessions) > 0 {
		resources, err := s.conn.Children(s.kb.currentStatesForSession(instance, sessions[0]))
		must(err)

		for _, r := range resources {
			s.watchCurrentStateOfInstanceForResource(instance, r, sessions[0])
		}
	}
}

func (s *Manager) watchCurrentStateOfInstanceForResource(instance string, resource string, sessionID string) {
	s.Lock()
	defer s.Unlock()

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
			must(err)
			select {
			case <-events:
				s.changeNotificationChan <- helix.ChangeNotification{helix.CurrentStateChanged, instance}
				continue
			case <-s.stopCurrentStateWatch[watchPath]:
				delete(s.stopCurrentStateWatch, watchPath)
				return
			}
		}
	}()
}

func (s *Manager) watchLiveInstances() {
	errors := make(chan error)

	go func() {
		for {
			_, events, err := s.conn.ChildrenW(s.kb.liveInstances())
			if err != nil {
				errors <- err
				return
			}

			// notify the live instance update
			s.changeNotificationChan <- helix.ChangeNotification{helix.LiveInstanceChanged, nil}

			// block the loop to wait for the live instance change
			evt := <-events
			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()
}

func (s *Manager) watchInstanceConfig() {
	errors := make(chan error)

	go func() {
		for {
			configs, events, err := s.conn.ChildrenW(s.kb.participantConfigs())
			if err != nil {
				errors <- err
				return
			}

			// find the resources that are newly added, and create a watcher
			for _, k := range configs {
				_, ok := s.instanceConfigMap[k]
				if !ok {
					s.watchInstanceConfigForParticipant(k)

					s.Lock()
					s.instanceConfigMap[k] = true
					s.Unlock()
				}
			}

			// refresh the instanceConfigMap to make sure only the currently existing resources
			// are marked as true
			s.Lock()
			for k := range s.instanceConfigMap {
				s.instanceConfigMap[k] = false
			}
			for _, k := range configs {
				s.instanceConfigMap[k] = true
			}
			s.Unlock()

			// Notify an update of external view if there are new resources added.
			s.changeNotificationChan <- helix.ChangeNotification{helix.InstanceConfigChanged, nil}

			// now need to block the loop to wait for the next update event
			evt := <-events
			if evt.Err != nil {
				panic(evt.Err)
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
			<-events
			s.changeNotificationChan <- helix.ChangeNotification{helix.InstanceConfigChanged, instance}
			must(err)
		}
	}()

}

func (s *Manager) watchIdealState() {
	errors := make(chan error)

	go func() {
		for {
			resources, events, err := s.conn.ChildrenW(s.kb.idealStates())
			if err != nil {
				errors <- err
				return
			}

			// find the resources that are newly added, and create a watcher
			for _, k := range resources {
				_, ok := s.idealStateResourceMap[k]
				if !ok {
					s.watchIdealStateResource(k)
					s.idealStateResourceMap[k] = true
				}
			}

			// refresh the idealStateResourceMap to make sure only the currently existing resources
			// are marked as true
			for k := range s.idealStateResourceMap {
				s.idealStateResourceMap[k] = false
			}
			for _, k := range resources {
				s.idealStateResourceMap[k] = true
			}

			// Notify an update of external view if there are new resources added.
			s.changeNotificationChan <- helix.ChangeNotification{helix.IdealStateChanged, nil}

			// now need to block the loop to wait for the next update event
			evt := <-events
			if evt.Err != nil {
				panic(evt.Err)
				return
			}
		}
	}()
}

func (s *Manager) watchExternalView() {
	errors := make(chan error)

	go func() {
		for {
			resources, events, err := s.conn.ChildrenW(s.kb.externalView())
			if err != nil {
				errors <- err
				return
			}

			// find the resources that are newly added, and create a watcher
			for _, k := range resources {
				_, ok := s.externalViewResourceMap[k]
				if !ok {
					s.watchExternalViewResource(k)
					s.externalViewResourceMap[k] = true
				}
			}

			// refresh the externalViewResourceMap to make sure only the currently existing resources
			// are marked as true
			for k := range s.externalViewResourceMap {
				s.externalViewResourceMap[k] = false
			}
			for _, k := range resources {
				s.externalViewResourceMap[k] = true
			}

			// Notify an update of external view if there are new resources added.
			s.changeNotificationChan <- helix.ChangeNotification{helix.ExternalViewChanged, ""}

			// now need to block the loop to wait for the next update event
			evt := <-events
			if evt.Err != nil {
				//panic(evt.Err)
				fmt.Println(evt.Err)
				return
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
		s.changeNotificationChan <- helix.ChangeNotification{helix.ControllerMessagesChanged, nil}

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

// loop is the main event loop for Spectator. Whenever an external view update happpened
// the loop will pause for a short period of time to bucket all subsequent external view
// changes so that we don't send duplicate updates too often.
func (s *Manager) startChangeNotificationLoop() {
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
		for {
			select {
			case <-s.stop:
				//s.state = spectatorDisConnected
				return

			case chg := <-s.changeNotificationChan:
				s.handleChangeNotification(chg)
				continue

			}
		}
	}()
}

func (s *Manager) handleChangeNotification(chg helix.ChangeNotification) {
	switch chg.ChangeType {
	case helix.ExternalViewChanged:
		ev := s.GetExternalView()
		if s.context != nil {
			s.context.Set("trigger", chg.ChangeData.(string))
		}

		for _, evListener := range s.externalViewListeners {
			go evListener(ev, s.context)
		}

	case helix.LiveInstanceChanged:
		li, _ := s.GetLiveInstances()
		for _, l := range s.liveInstanceChangeListeners {
			go l(li, s.context)
		}

	case helix.IdealStateChanged:
		is := s.GetIdealState()

		for _, isListener := range s.idealStateChangeListeners {
			go isListener(is, s.context)
		}

	case helix.CurrentStateChanged:
		instance := chg.ChangeData.(string)
		cs := s.GetCurrentState(instance)
		for _, listener := range s.currentStateChangeListeners[instance] {
			go listener(instance, cs, s.context)
		}

	case helix.InstanceConfigChanged:
		ic := s.GetInstanceConfigs()
		for _, icListener := range s.instanceConfigChangeListeners {
			go icListener(ic, s.context)
		}

	case helix.ControllerMessagesChanged:
		cm := s.GetControllerMessages()
		for _, cmListener := range s.controllerMessageListeners {
			go cmListener(cm, s.context)
		}

	case helix.InstanceMessagesChanged:
		instance := chg.ChangeData.(string)
		messageRecords := s.GetInstanceMessages(instance)
		for _, ml := range s.messageListeners[instance] {
			go ml(instance, messageRecords, s.context)
		}
	}
}
