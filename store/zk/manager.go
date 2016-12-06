package zk

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
	lru "github.com/hashicorp/golang-lru"
	"github.com/yichen/go-zookeeper/zk"
)

var _ helix.HelixManager = &Manager{}

type Manager struct {
	sync.RWMutex

	closeOnce sync.Once

	zkSvr     string
	conn      *connection
	connected bool
	stop      chan struct{}

	ClusterID string
	it        helix.InstanceType
	kb        keyBuilder

	// only participant holds a state machine engine
	sme *stateMachineEngine

	// host of this participant
	Host string

	// port of this participant
	Port string

	// ParticipantID is the optional identifier of this participant, by default to host_port
	ParticipantID string

	// context of the specator, accessible from the ExternalViewChangeListener
	context *helix.Context

	externalViewListeners       []helix.ExternalViewChangeListener
	liveInstanceChangeListeners []helix.LiveInstanceChangeListener
	currentStateChangeListeners map[string][]helix.CurrentStateChangeListener
	idealStateChangeListeners   []helix.IdealStateChangeListener
	messageListeners            map[string][]helix.MessageListener

	// not implemented
	controllerMessageListeners    []helix.ControllerMessageListener
	instanceConfigChangeListeners []helix.InstanceConfigChangeListener

	// resources the external view is tracking.
	// It is a map from the resource name to the current state of the resource:
	// true means it is active, false means the resource is inactive/deleted
	externalViewResourceMap map[string]bool
	idealStateResourceMap   map[string]bool
	instanceConfigMap       map[string]bool

	// changeNotification is a channel to notify any changes that needs to trigger a listener
	changeNotificationChan chan helix.ChangeNotification

	// instance message channel.
	// Each item in the channel is the instance name that has new messages
	instanceMessageChannel chan string

	// a LRU cache of recently received message IDs.
	// Use this to detect new messages and existing messages
	receivedMessages *lru.Cache

	// control channels for stopping watches
	stopCurrentStateWatch map[string]chan interface{}
}

// NewZKHelixManager creates a HelixManager implementation with zk as storage.
func NewZKHelixManager(clusterID, host, port, zkSvr string, it helix.InstanceType, options ...zkConnOption) helix.HelixManager {
	mgr := &Manager{
		zkSvr:     zkSvr,
		ClusterID: clusterID,
		conn:      newConnection(zkSvr),
		stop:      make(chan struct{}),

		it:            it,
		kb:            keyBuilder{clusterID: clusterID},
		Host:          host,
		Port:          port,
		ParticipantID: fmt.Sprintf("%s_%s", host, port), // node id

		// listeners
		externalViewListeners:       []helix.ExternalViewChangeListener{},
		liveInstanceChangeListeners: []helix.LiveInstanceChangeListener{},
		currentStateChangeListeners: map[string][]helix.CurrentStateChangeListener{},
		idealStateChangeListeners:   []helix.IdealStateChangeListener{},
		messageListeners:            map[string][]helix.MessageListener{},

		// control channels
		externalViewResourceMap: map[string]bool{},
		idealStateResourceMap:   map[string]bool{},
		instanceConfigMap:       map[string]bool{},

		changeNotificationChan: make(chan helix.ChangeNotification, 1000),

		stopCurrentStateWatch: make(map[string]chan interface{}),

		// channel for receiving instance messages
		instanceMessageChannel: make(chan string, 100),
	}
	for _, option := range options {
		option(mgr.conn)
	}

	switch it {
	case helix.InstanceTypeParticipant:
		mgr.sme = newStateMachineEngine(mgr)

	case helix.InstanceTypeSpectator:
	}

	return mgr
}

func (m *Manager) Connect() error {
	m.RLock()
	if m.connected {
		m.RUnlock()
		return nil
	}
	m.RUnlock()

	m.Lock()
	defer m.Unlock()
	if m.connected {
		return nil
	}

	log.Trace("manager{cluster:%s, instance:%s, type:%s, zk:%s} connecting...",
		m.ClusterID, m.ParticipantID, m.it, m.zkSvr)

	if err := m.conn.Connect(); err != nil {
		return err
	}

	// handleNewSessionAsController, handleNewSessionAsParticipant
	// handleStateChanged

	if ok, err := m.conn.IsClusterSetup(m.ClusterID); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	if m.it.IsParticipant() {
		if ok, err := m.joinCluster(); !ok || err != nil {
			if err != nil {
				return err
			}
			return helix.ErrEnsureParticipantConfig
		}

		if err := m.cleanUpStaleSessions(); err != nil {
			return err
		}

		m.startEventLoop()

		// TODO sync between watcher and live instances

		m.createLiveInstance()
	}

	m.loop()

	m.connected = true
	return nil
}

func (m *Manager) IsConnected() bool {
	return true
}

func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.Lock()
		if m.connected {
			m.conn.Disconnect()
			m.connected = false
		}
		m.Unlock()
	})
}

func (m *Manager) StateMachineEngine() helix.StateMachineEngine {
	return m.sme
}

func (m *Manager) SetContext(context *helix.Context) {
	m.Lock()
	m.context = context
	m.Unlock()
}

func (p *Manager) cleanUpStaleSessions() error {
	log.Trace("P[%s/%s] cleanup stale sessions", p.ParticipantID, p.conn.GetSessionID())

	sessions, err := p.conn.Children(p.kb.currentStates(p.ParticipantID))
	if err != nil {
		return err
	}

	for _, sessionID := range sessions {
		if sessionID != p.conn.GetSessionID() {
			log.Warn("P[%s/%s] found stale session: %s", p.ParticipantID, p.conn.GetSessionID(), sessionID)

			if err = p.conn.DeleteTree(p.kb.currentStatesForSession(p.ParticipantID, sessionID)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *Manager) autoJoinAllowed() (bool, error) {
	config, err := p.conn.Get(p.kb.clusterConfig())
	if err != nil {
		return false, err
	}

	c, err := model.NewRecordFromBytes(config)
	if err != nil {
		return false, err
	}

	allowed := c.GetSimpleField("allowParticipantAutoJoin")
	if allowed == nil {
		// false by default
		return false, nil
	}

	al, _ := allowed.(string)
	return strings.ToLower(al) == "true", nil
}

func (p *Manager) joinCluster() (bool, error) {
	// /{cluster}/CONFIGS/PARTICIPANT/localhost_12000
	exists, err := p.conn.Exists(p.kb.participantConfig(p.ParticipantID))
	if err != nil {
		return false, err
	}

	if exists {
		return true, nil
	}

	allowAutoJoin, err := p.autoJoinAllowed()
	if err != nil {
		return false, err
	}
	if !allowAutoJoin {
		return false, nil
	}

	// the participant path does not exist in zookeeper
	// create the data struture
	participant := model.NewRecord(p.ParticipantID)
	participant.SetSimpleField("HELIX_HOST", p.Host)
	participant.SetSimpleField("HELIX_PORT", p.Port)
	participant.SetSimpleField("HELIX_ENABLED", "true")
	err = any(
		p.conn.CreateRecordWithPath(p.kb.participantConfig(p.ParticipantID), participant),

		// /{cluster}/INSTANCES/localhost_12000
		p.conn.CreateEmptyNode(p.kb.instance(p.ParticipantID)),

		// /{cluster}/INSTANCES/localhost_12000/CURRENTSTATES
		p.conn.CreateEmptyNode(p.kb.currentStates(p.ParticipantID)),

		// /{cluster}/INSTANCES/localhost_12000/ERRORS
		p.conn.CreateEmptyNode(p.kb.errorsR(p.ParticipantID)),

		// /{cluster}/INSTANCES/localhost_12000/HEALTHREPORT
		p.conn.CreateEmptyNode(p.kb.healthReport(p.ParticipantID)),

		// /{cluster}/INSTANCES/localhost_12000/MESSAGES
		p.conn.CreateEmptyNode(p.kb.messages(p.ParticipantID)),

		// /{cluster}/INSTANCES/localhost_12000/STATUSUPDATES
		p.conn.CreateEmptyNode(p.kb.statusUpdates(p.ParticipantID)),
	)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (p *Manager) startEventLoop() {
	log.Trace("P[%s/%s] starting main loop", p.ParticipantID, p.conn.GetSessionID())

	messageProcessedTime := make(map[string]time.Time)
	go func() {
		tick := time.NewTicker(time.Second * 5)
		defer tick.Stop()

		for {
			select {
			case now := <-tick.C:
				for k, v := range messageProcessedTime {
					if now.Sub(v).Seconds() > 10 {
						// FIXME concurrent modify
						delete(messageProcessedTime, k)
					}
				}

			case <-p.stop:
				return
			}
		}
	}()

	messagesChan, errChan := p.watchMessages()
	go func() {

		for {
			select {
			case m := <-messagesChan:
				for _, msg := range m {
					// messageChan is a snapshot of all unprocessed messages whenever
					// a new message is added, so it will have duplicates.
					if _, seen := messageProcessedTime[msg]; !seen {
						p.processMessage(msg)
						messageProcessedTime[msg] = time.Now()
					}
				}

			case err := <-errChan:
				log.Error("P[%s/%s] %v", p.ParticipantID, p.conn.GetSessionID(), err)

			case <-p.stop:
				log.Trace("P[%s/%s] stopped", p.ParticipantID, p.conn.GetSessionID())
				return
			}
		}
	}()
}

func (p *Manager) watchMessages() (chan []string, chan error) {
	snapshots := make(chan []string)
	errors := make(chan error)

	go func() {
		path := p.kb.messages(p.ParticipantID)
		for {
			snapshot, events, err := p.conn.ChildrenW(path)
			if err != nil {
				errors <- err
				return
			}

			snapshots <- snapshot
			evt := <-events
			log.Debug("P[%s/%s] recv message: %+v %+v", p.ParticipantID, p.conn.GetSessionID(), snapshot, evt)

			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()

	return snapshots, errors
}

func (p *Manager) createLiveInstance() error {
	record := model.NewLiveInstanceRecord(p.ParticipantID, p.conn.GetSessionID())
	data, err := json.MarshalIndent(*record, "", "  ")
	flags := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)

	// it is possible the live instance still exists from last run
	// retry 5 seconds to wait for the zookeeper to remove the live instance
	// from previous session
	for retry := 0; retry < 10; retry++ {
		_, err = p.conn.Create(p.kb.liveInstance(p.ParticipantID), data, flags, acl)
		if err == nil {
			log.Trace("P[%s/%s] become alive", p.ParticipantID, p.conn.GetSessionID())
			return nil
		}

		// wait for zookeeper remove the last run's ephemeral znode
		time.Sleep(time.Second * 5)

	}

	return err
}

// handleClusterMessage dispatches the cluster message to the corresponding
// handler in the state model.
// message content example:
// 9ff57fc1-9f2a-41a5-af46-c4ae2a54c539
// {
//     "id": "9ff57fc1-9f2a-41a5-af46-c4ae2a54c539",
//     "simpleFields": {
//         "CREATE_TIMESTAMP": "1425268051457",
//         "ClusterEventName": "currentStateChange",
//         "FROM_STATE": "OFFLINE",
//         "MSG_ID": "9ff57fc1-9f2a-41a5-af46-c4ae2a54c539",
//         "MSG_STATE": "new",
//         "MSG_TYPE": "STATE_TRANSITION",
//         "PARTITION_NAME": "myDB_5",
//         "RESOURCE_NAME": "myDB",
//         "SRC_NAME": "precise64-CONTROLLER",
//         "SRC_SESSION_ID": "14bd852c528004c",
//         "STATE_MODEL_DEF": "MasterSlave",
//         "STATE_MODEL_FACTORY_NAME": "DEFAULT",
//         "TGT_NAME": "localhost_12913",
//         "TGT_SESSION_ID": "93406067297878252",
//         "TO_STATE": "SLAVE"
//     },
//     "listFields": {},
//     "mapFields": {}
// }
//
func (p *Manager) processMessage(msgID string) error {
	log.Debug("P[%s/%s] processing msg: %s", p.ParticipantID, p.conn.GetSessionID(), msgID)

	msgPath := p.kb.message(p.ParticipantID, msgID)
	record, err := p.conn.GetRecordFromPath(msgPath)
	if err != nil {
		return err
	}

	message := model.NewMessageFromRecord(record)

	log.Debug("P[%s/%s] message: %s", p.ParticipantID, p.conn.GetSessionID(), message)

	msgType := message.MessageType()
	if msgType == helix.MessageTypeNoOp {
		log.Warn("P[%s/%s] discard NO-OP message: %s", p.ParticipantID, p.conn.GetSessionID(), msgID)
		return p.conn.DeleteTree(msgPath)
	}

	sessionID := message.TargetSessionID()
	if sessionID != "*" && sessionID != p.conn.GetSessionID() {
		// message comes from expired session
		log.Warn("P[%s/%s] got mismatched message: %s", p.ParticipantID, p.conn.GetSessionID(), message)
		return p.conn.DeleteTree(msgPath)
	}

	if !strings.EqualFold(message.MessageState(), helix.MessageStateNew) {
		// READ message is not deleted until the state has changed
		log.Warn("P[%s/%s] skip %s message: %s", p.ParticipantID, p.conn.GetSessionID(), message.MessageState(), msgID)
		return nil
	}

	// update msgState to read
	message.SetSimpleField("MSG_STATE", helix.MessageStateRead)
	message.SetSimpleField("READ_TIMESTAMP", time.Now().Unix())
	message.SetSimpleField("EXE_SESSION_ID", p.conn.GetSessionID())

	// create current state meta data
	// do it for non-controller and state transition messages only
	targetName := message.TargetName()
	if !strings.EqualFold(targetName, "CONTROLLER") && strings.EqualFold(msgType, "STATE_TRANSITION") {
		resourceID := message.Resource()

		currentStateRecord := model.NewRecord(resourceID)
		currentStateRecord.SetIntField("BUCKET_SIZE", message.GetIntField("BUCKET_SIZE", 0))
		currentStateRecord.SetSimpleField("STATE_MODEL_DEF", message.GetSimpleField("STATE_MODEL_DEF"))
		currentStateRecord.SetSimpleField("SESSION_ID", sessionID)
		currentStateRecord.SetBooleanField("BATCH_MESSAGE_MODE", message.GetBooleanField("BATCH_MESSAGE_MODE", false))
		currentStateRecord.SetSimpleField("STATE_MODEL_FACTORY_NAME", message.GetStringField("STATE_MODEL_FACTORY_NAME", "DEFAULT"))

		resourceCurrentStatePath := p.kb.currentStateForResource(p.ParticipantID, sessionID, resourceID)
		exists, err := p.conn.Exists(resourceCurrentStatePath)
		if err != nil {
			return err
		}

		if !exists {
			// only set the current state to zookeeper when it is not present
			if err = p.conn.SetRecordForPath(resourceCurrentStatePath, currentStateRecord); err != nil {
				return err
			}
		}
	}

	p.handleStateTransition(message)

	// after the message is processed successfully, remove it
	return p.conn.DeleteTree(msgPath)
}

func (p *Manager) handleStateTransition(message *model.Message) {
	log.Trace("P[%s/%s] state %s -> %s", p.ParticipantID,
		p.conn.GetSessionID(), message.FromState(), message.ToState())

	// set the message execution time
	nowMilli := time.Now().UnixNano() / 1000000
	startTime := strconv.FormatInt(nowMilli, 10)
	message.SetSimpleField("EXECUTE_START_TIMESTAMP", startTime)

	p.preHandleMessage(message)

	// TODO lock
	transition, present := p.sme.StateModel(message.StateModel())
	if !present {
		log.Error("P[%s/%s] has no transition defined for state model %s", p.ParticipantID,
			p.conn.GetSessionID(), message.StateModel())
	} else {
		handler := transition.Handler(message.FromState(), message.ToState())
		if handler == nil {
			log.Warn("P[%s/%s] state %s -> %s has no handler", p.ParticipantID,
				p.conn.GetSessionID(), message.FromState(), message.ToState())
		} else {
			context := helix.NewContext()
			handler(message, context)
		}
	}

	p.postHandleMessage(message)
}

func (p *Manager) preHandleMessage(message *model.Message) {

}

func (p *Manager) postHandleMessage(message *model.Message) error {
	// sessionID might change when we update the state model
	// skip if we are handling an expired session
	sessionID := p.conn.GetSessionID()
	targetSessionID := message.TargetSessionID()
	toState := message.ToState()
	partitionName := message.PartitionName()

	if targetSessionID != sessionID {
		return helix.ErrSessionChanged
	}

	// if the target state is DROPPED, we need to remove the resource key
	// from the current state of the instance because the resource key is dropped.
	// In the state model it will be stayed as OFFLINE, which is OK.

	if strings.ToUpper(toState) == "DROPPED" {
		p.conn.RemoveMapFieldKey(p.kb.currentStatesForSession(p.ParticipantID, sessionID), partitionName)
	}

	// actually set the current state
	currentStateForResourcePath := p.kb.currentStateForResource(p.ParticipantID,
		p.conn.GetSessionID(), message.Resource())
	return p.conn.UpdateMapField(currentStateForResourcePath, partitionName,
		"CURRENT_STATE", toState)
}

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

func (s *Manager) AddCurrentStateChangeListener(instance string, listener helix.CurrentStateChangeListener) {
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

func (s *Manager) watchExternalViewResource(resource string) {
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

func (s *Manager) watchIdealStateResource(resource string) {
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
func (s *Manager) GetControllerMessages() []*model.Record {
	result := []*model.Record{}

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
func (s *Manager) GetInstanceMessages(instance string) []*model.Record {
	result := []*model.Record{}

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
func (s *Manager) GetLiveInstances() ([]*model.Record, error) {
	liveInstances := []*model.Record{}

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
func (s *Manager) GetExternalView() []*model.Record {
	result := []*model.Record{}

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
func (s *Manager) GetIdealState() []*model.Record {
	result := []*model.Record{}

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
func (s *Manager) GetCurrentState(instance string) []*model.Record {
	result := []*model.Record{}

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
func (s *Manager) GetInstanceConfigs() []*model.Record {
	result := []*model.Record{}

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
func (s *Manager) loop() {
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
