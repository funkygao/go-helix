package zk

import (
	"fmt"
	"sync"
	"time"

	"github.com/funkygao/go-helix"
	lru "github.com/hashicorp/golang-lru"
)

var _ helix.HelixSpectator = &Spectator{}

type spectatorState uint8

const (
	spectatorConnected    spectatorState = 0
	spectatorDisConnected spectatorState = 1
)

// Spectator is a Helix role that does not participate the cluster state transition
// but only read cluster data, or listen to cluster updates
type Spectator struct {
	sync.RWMutex

	conn *connection

	// The cluster this spectator is specatating
	ClusterID string

	// external view change handler
	externalViewListeners         []helix.ExternalViewChangeListener
	liveInstanceChangeListeners   []helix.LiveInstanceChangeListener
	currentStateChangeListeners   map[string][]helix.CurrentStateChangeListener
	idealStateChangeListeners     []helix.IdealStateChangeListener
	instanceConfigChangeListeners []helix.InstanceConfigChangeListener
	controllerMessageListeners    []helix.ControllerMessageListener
	messageListeners              map[string][]helix.MessageListener

	// stop the spectator
	stop chan bool

	kb keyBuilder

	// resources the external view is tracking. It is a map from the resource name to the
	// current state of the resource: true means it is active, false means the resource is inactive/deleted
	externalViewResourceMap map[string]bool
	idealStateResourceMap   map[string]bool
	instanceConfigMap       map[string]bool

	// changeNotification is a channel to notify any changes that needs to trigger a listener
	changeNotificationChan chan helix.ChangeNotification

	// instance message channel. Each item in the channel is the instance name that has new messages
	instanceMessageChannel chan string

	// a LRU cache of recently received message IDs. Use this to detect new messages and existing messages
	receivedMessages *lru.Cache

	// control channels for stopping watches
	stopCurrentStateWatch map[string]chan interface{}

	// context of the specator, accessible from the ExternalViewChangeListener
	context *helix.Context

	state spectatorState
}

func (s *Spectator) Start() error {
	if s.conn == nil || !s.conn.IsConnected() {
		// Manager is responsible for connection
		return helix.ErrNotConnected
	}

	if ok, err := s.conn.IsClusterSetup(s.ClusterID); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	// start the event loop for spectator
	s.loop()

	s.state = spectatorConnected
	return nil
}

// Disconnect the spectator from zookeeper, and also stop all listeners.
func (s *Spectator) Close() {
	if s.state == spectatorDisConnected {
		return
	}

	// wait for graceful shutdown of the external view listener
	if s.state != spectatorDisConnected {
		s.stop <- true
		close(s.stop)
	}

	for s.state != spectatorDisConnected {
		time.Sleep(100 * time.Millisecond)
	}

	s.state = spectatorDisConnected
}

// IsConnected test if the spectator is connected
func (s *Spectator) IsConnected() bool {
	return s.state == spectatorConnected
}

// SetContext set the context that can be used within the listeners.
func (s *Spectator) SetContext(context *helix.Context) {
	s.Lock()
	s.context = context
	s.Unlock()
}

func (s *Spectator) AddExternalViewChangeListener(listener helix.ExternalViewChangeListener) {
	s.Lock()
	s.externalViewListeners = append(s.externalViewListeners, listener)
	s.Unlock()
}

func (s *Spectator) AddLiveInstanceChangeListener(listener helix.LiveInstanceChangeListener) {
	s.Lock()
	s.liveInstanceChangeListeners = append(s.liveInstanceChangeListeners, listener)
	s.Unlock()
}

func (s *Spectator) AddCurrentStateChangeListener(instance string, listener helix.CurrentStateChangeListener) {
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

func (s *Spectator) AddMessageListener(instance string, listener helix.MessageListener) {
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

func (s *Spectator) AddIdealStateChangeListener(listener helix.IdealStateChangeListener) {
	s.Lock()
	s.idealStateChangeListeners = append(s.idealStateChangeListeners, listener)
	s.Unlock()
}

func (s *Spectator) AddInstanceConfigChangeListener(listener helix.InstanceConfigChangeListener) {
	s.Lock()
	s.instanceConfigChangeListeners = append(s.instanceConfigChangeListeners, listener)
	s.Unlock()
}

func (s *Spectator) AddControllerMessageListener(listener helix.ControllerMessageListener) {
	s.Lock()
	s.controllerMessageListeners = append(s.controllerMessageListeners, listener)
	s.Unlock()
}

func (s *Spectator) watchExternalViewResource(resource string) {
	go func() {
		for {
			// block and wait for the next update for the resource
			// when the update happens, unblock, and also send the resource
			// to the channel
			_, events, err := s.conn.GetW(s.kb.externalViewForResource(resource))
			<-events
			s.changeNotificationChan <- helix.ChangeNotification{helix.ExternalViewChanged, resource}
			must(err)
		}
	}()
}

func (s *Spectator) watchIdealStateResource(resource string) {
	go func() {
		for {
			// block and wait for the next update for the resource
			// when the update happens, unblock, and also send the resource
			// to the channel
			_, events, err := s.conn.GetW(s.kb.idealStateForResource(resource))
			<-events
			s.changeNotificationChan <- helix.ChangeNotification{helix.IdealStateChanged, resource}
			must(err)
		}
	}()
}

// GetControllerMessages retrieves controller messages from zookeeper
func (s *Spectator) GetControllerMessages() []*helix.Record {
	result := []*helix.Record{}

	messages, err := s.conn.Children(s.kb.controllerMessages())
	if err != nil {
		return result
	}

	for _, m := range messages {
		record, err := s.conn.GetRecordFromPath(s.kb.controllerMessage(m))
		if err == nil {
			result = append(result, record)
		} else {
			// TODO handle the err
		}
	}

	return result
}

// GetInstanceMessages retrieves messages sent to an instance
func (s *Spectator) GetInstanceMessages(instance string) []*helix.Record {
	result := []*helix.Record{}

	messages, err := s.conn.Children(s.kb.messages(instance))
	if err != nil {
		return result
	}

	for _, m := range messages {
		record, err := s.conn.GetRecordFromPath(s.kb.message(instance, m))
		if err == nil {
			result = append(result, record)
		} else {
			// TODO
		}
	}

	return result
}

// GetLiveInstances retrieve a copy of the current live instances.
func (s *Spectator) GetLiveInstances() ([]*helix.Record, error) {
	liveInstances := []*helix.Record{}

	instances, err := s.conn.Children(s.kb.liveInstances())
	if err != nil {
		return nil, err
	}

	for _, participantID := range instances {
		r, err := s.conn.GetRecordFromPath(s.kb.liveInstance(participantID))
		if err != nil {
			return liveInstances, err
		}

		liveInstances = append(liveInstances, r)
	}

	return liveInstances, nil
}

// GetExternalView retrieves a copy of the external views
func (s *Spectator) GetExternalView() []*helix.Record {
	result := []*helix.Record{}

	for k, v := range s.externalViewResourceMap {
		if v == false {
			continue
		}

		record, err := s.conn.GetRecordFromPath(s.kb.externalViewForResource(k))

		if err == nil {
			result = append(result, record)
			continue
		}
	}

	return result
}

// GetIdealState retrieves a copy of the ideal state
func (s *Spectator) GetIdealState() []*helix.Record {
	result := []*helix.Record{}

	for k, v := range s.idealStateResourceMap {
		if v == false {
			continue
		}

		record, err := s.conn.GetRecordFromPath(s.kb.idealStateForResource(k))

		if err == nil {
			result = append(result, record)
			continue
		}
	}
	return result
}

// GetCurrentState retrieves a copy of the current state for specified instance
func (s *Spectator) GetCurrentState(instance string) []*helix.Record {
	result := []*helix.Record{}

	resources, err := s.conn.Children(s.kb.instance(instance))
	must(err)

	for _, r := range resources {
		record, err := s.conn.GetRecordFromPath(s.kb.currentStateForResource(instance, s.conn.GetSessionID(), r))
		if err == nil {
			result = append(result, record)
		}
	}

	return result
}

// GetInstanceConfigs retrieves a copy of instance configs from zookeeper
func (s *Spectator) GetInstanceConfigs() []*helix.Record {
	result := []*helix.Record{}

	configs, err := s.conn.Children(s.kb.participantConfigs())
	must(err)

	for _, i := range configs {
		record, err := s.conn.GetRecordFromPath(s.kb.participantConfig(i))
		if err == nil {
			result = append(result, record)
		}
	}

	return result
}

func (s *Spectator) watchCurrentStates() {
	for k := range s.currentStateChangeListeners {
		s.watchCurrentStateForInstance(k)
	}
}

func (s *Spectator) watchCurrentStateForInstance(instance string) {
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

func (s *Spectator) watchCurrentStateOfInstanceForResource(instance string, resource string, sessionID string) {
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

func (s *Spectator) watchLiveInstances() {
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

func (s *Spectator) watchInstanceConfig() {
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

func (s *Spectator) watchInstanceConfigForParticipant(instance string) {
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

func (s *Spectator) watchIdealState() {
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

func (s *Spectator) watchExternalView() {
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
func (s *Spectator) watchControllerMessages() {
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

func (s *Spectator) watchInstanceMessages(instance string) {
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
func (s *Spectator) watchInstanceMessage(instance string, messageID string) {
	go func() {

	}()
}

// loop is the main event loop for Spectator. Whenever an external view update happpened
// the loop will pause for a short period of time to bucket all subsequent external view
// changes so that we don't send duplicate updates too often.
func (s *Spectator) loop() {
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
				s.state = spectatorDisConnected
				return

			case chg := <-s.changeNotificationChan:
				s.handleChangeNotification(chg)
				continue

			}
		}
	}()
}

func (s *Spectator) handleChangeNotification(chg helix.ChangeNotification) {
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
