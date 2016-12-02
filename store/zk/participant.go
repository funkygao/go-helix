package zk

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/go-helix"
	log "github.com/funkygao/log4go"
	"github.com/yichen/go-zookeeper/zk"
)

type participantState uint8

const (
	psConnected    participantState = 0
	psStarted      participantState = 1
	psStopped      participantState = 2
	psDisconnected participantState = 3
)

var _ helix.HelixParticipant = &Participant{}

type Participant struct {
	sync.Mutex

	manger helix.HelixManager

	conn *connection
	kb   keyBuilder

	// The cluster this participant belongs to
	ClusterID string

	// host of this participant
	Host string

	// port of this participant
	Port string

	// ParticipantID is the optional identifier of this participant, by default to host_port
	ParticipantID string

	// an instance of StateModel
	stateModels map[string]*helix.StateModel

	// channel to receive upon start of event loop
	started chan interface{}
	// channel to receive stop participant event
	stop chan bool

	state participantState
}

func (p *Participant) Start() error {
	if len(p.stateModels) == 0 {
		return helix.ErrEmptyStateModel
	}

	if p.conn == nil || !p.conn.IsConnected() {
		// Manager is responsible for connection
		return helix.ErrNotConnected
	}

	if ok, err := p.conn.IsClusterSetup(p.ClusterID); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	if ok, err := p.participantRegistered(); !ok || err != nil {
		if err != nil {
			return err
		}
		return helix.ErrEnsureParticipantConfig
	}

	if err := p.cleanUpStaleSessions(); err != nil {
		return err
	}

	p.startEventLoop()

	// TODO sync between watcher and live instances

	p.createLiveInstance()

	return nil
}

func (p *Participant) cleanUpStaleSessions() error {
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

// Disconnect the participant from Zookeeper and Helix controller.
func (p *Participant) Close() {
	// do i need lock here?
	if p.state == psDisconnected {
		return
	}

	// if the state is connected, it means we are not in event loop
	// if the state is started, it means we are in event loop and need to sent
	// the stop message
	// if the status is started, it means the event loop is running
	// wait for it to stop
	if p.state == psStarted {
		p.stop <- true
		close(p.stop)
		for p.state != psStopped {
			time.Sleep(100 * time.Millisecond)
		}
	}

	/* TODO
	p.conn.Delete(p.kb.liveInstance(p.ParticipantID))
	if p.conn != nil && p.conn.IsConnected() {
		p.conn.Disconnect()
	}
	*/

	p.state = psDisconnected
}

func (p *Participant) RegisterStateModel(name string, sm *helix.StateModel) error {
	p.Lock()
	defer p.Unlock()

	if p.stateModels == nil {
		p.stateModels = make(map[string]*helix.StateModel)
	} else if _, present := p.stateModels[name]; present {
		return helix.ErrDupStateModelName
	}
	p.stateModels[name] = sm
	return nil
}

func (p *Participant) autoJoinAllowed() (bool, error) {
	config, err := p.conn.Get(p.kb.clusterConfig())
	if err != nil {
		return false, err
	}

	c, err := helix.NewRecordFromBytes(config)
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

func (p *Participant) participantRegistered() (bool, error) {
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
	participant := helix.NewRecord(p.ParticipantID)
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

func (p *Participant) startEventLoop() {
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
		// psStarted means the message loop is running, and
		// it can process p.stop message
		p.state = psStarted

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
				log.Error("P[%s] %v", p.ParticipantID, err)

			case <-p.stop:
				p.state = psStopped
				return
			}
		}
	}()
}

func (p *Participant) watchMessages() (chan []string, chan error) {
	snapshots := make(chan []string)
	errors := make(chan error)
	path := p.kb.messages(p.ParticipantID)

	go func() {
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

func (p *Participant) createLiveInstance() error {
	record := helix.NewLiveInstanceRecord(p.ParticipantID, p.conn.GetSessionID())
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
func (p *Participant) processMessage(msgID string) error {
	log.Debug("P[%s/%s] processing msg: %s", p.ParticipantID, p.conn.GetSessionID(), msgID)

	msgPath := p.kb.message(p.ParticipantID, msgID)
	record, err := p.conn.GetRecordFromPath(msgPath)
	if err != nil {
		return err
	}

	message := helix.NewMessageFromRecord(record)

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

		currentStateRecord := helix.NewRecord(resourceID)
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

func (p *Participant) handleStateTransition(message *helix.Message) {
	log.Trace("P[%s/%s] state %s -> %s", p.ParticipantID,
		p.conn.GetSessionID(), message.FromState(), message.ToState())

	// set the message execution time
	nowMilli := time.Now().UnixNano() / 1000000
	startTime := strconv.FormatInt(nowMilli, 10)
	message.SetSimpleField("EXECUTE_START_TIMESTAMP", startTime)

	p.preHandleMessage(message)

	// TODO lock
	transition, present := p.stateModels[message.StateModel()]
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

func (p *Participant) preHandleMessage(message *helix.Message) {

}

func (p *Participant) postHandleMessage(message *helix.Message) error {
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
