package zk

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/go-helix"
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

// Participant is a Helix participant node
type Participant struct {
	conn *connection

	// zookeeper connection string
	zkSvr string

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
	// channel to stop watch messages
	stopWatch chan bool

	// status
	state participantState

	// keybuilder
	kb keyBuilder

	sync.Mutex
}

// Connect the participant. Before connecting we need to validate that
// Zookeeper address are valid and also that the state models are registered.
// after connecting Zookeeper, we also need to register this participant
// with the cluster in Helix, and get ready to receive cluster messages.
func (p *Participant) Connect() error {
	if len(p.stateModels) == 0 {
		return helix.ErrEmptyStateModel
	}

	if p.conn == nil || !p.conn.IsConnected() {
		p.conn = newConnection(p.zkSvr)
		if err := p.conn.Connect(); err != nil {
			return err
		}
	}

	if ok, err := p.conn.IsClusterSetup(p.ClusterID); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	// register the participant with the cluster
	allowed := p.ensureParticipantConfig()
	if !allowed {
		p.Close()
		return helix.ErrEnsureParticipantConfig
	}

	// clean up current state of previous sessions
	if err := p.cleanUp(); err != nil {
		return err
	}

	p.startEventLoop()

	// TODO sync between watcher and live instances

	// bring this participant alive.
	p.createLiveInstance()

	return nil
}

func (p *Participant) cleanUp() error {
	sessions, err := p.conn.Children(p.kb.currentStates(p.ParticipantID))
	if err != nil {
		return err
	}

	for _, sessionID := range sessions {
		if sessionID != p.conn.GetSessionID() {
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

	if p.conn != nil && p.conn.IsConnected() {
		p.conn.Disconnect()
	}

	p.state = psDisconnected
}

// RegisterStateModel associates state trasition functions with the participant
func (p *Participant) RegisterStateModel(name string, sm helix.StateModel) {
	if p.stateModels == nil {
		p.stateModels = make(map[string]*helix.StateModel)
	}
	p.stateModels[name] = &sm
}

func (p *Participant) autoJoinAllowed() bool {
	key := p.kb.clusterConfig()
	config, err := p.conn.Get(key)
	must(err)

	c, err := helix.NewRecordFromBytes(config)
	must(err)

	allowed := c.GetSimpleField("allowParticipantAutoJoin")
	if allowed == nil {
		return false
	}

	al := allowed.(string)
	if strings.ToLower(al) == "true" {
		return true
	}
	return false
}

func (p *Participant) ensureParticipantConfig() bool {
	// make sure the participant confis exists in zookeeper
	key := p.kb.participantConfig(p.ParticipantID)
	exists, err := p.conn.Exists(key)
	if err != nil {
		return false
	}

	allowJoin := p.autoJoinAllowed()

	// if the participant path does not exist in zookeeper
	// create the data struture
	if !exists && allowJoin {
		participant := helix.NewRecord(p.ParticipantID)
		participant.SetSimpleField("HELIX_HOST", p.Host)
		participant.SetSimpleField("HELIX_PORT", p.Port)
		participant.SetSimpleField("HELIX_ENABLED", "true")

		p.conn.CreateRecordWithPath(key, participant)

		instance := p.kb.instance(p.ParticipantID)
		p.conn.CreateEmptyNode(instance)

		currentstates := p.kb.currentStates(p.ParticipantID)
		p.conn.CreateEmptyNode(currentstates)

		// errs := p.kb.errors(p.ParticipantID, strconv.FormatInt(p.zkConn.SessionID, 10), "")
		// createEmptyNode(p.zkConn, errs)
		errs := p.kb.errorsR(p.ParticipantID)
		p.conn.CreateEmptyNode(errs)

		health := p.kb.healthReport(p.ParticipantID)
		p.conn.CreateEmptyNode(health)

		messages := p.kb.messages(p.ParticipantID)
		p.conn.CreateEmptyNode(messages)

		updates := p.kb.statusUpdates(p.ParticipantID)
		p.conn.CreateEmptyNode(updates)
	} else if !exists {
		return false
	}

	return true
}

// main event loop for the participant. It listens to the participant message in zookeeper
// and for each update (messageChan), iterate all messages and process them
func (p *Participant) startEventLoop() {
	// we need to keep a history of the messages that have been processed, so we don't process
	// them again. Start a goroutine to clean up this history once every 5 seconds so it won't
	// take too much memory
	messageProcessedTime := make(map[string]time.Time)
	go func() {
		for {
			select {
			case <-time.After(5 * time.Second):
				for k, v := range messageProcessedTime {
					if time.Since(v).Seconds() > 10 {
						delete(messageProcessedTime, k)
					}
				}
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
				continue
			case err := <-errChan:
				fmt.Println(err.Error())
			case <-p.stop:
				p.state = psStopped
				return
			}
		}
	}()
}

func (p *Participant) createLiveInstance() error {
	path := p.kb.liveInstance(p.ParticipantID)
	node := helix.NewLiveInstanceNode(p.ParticipantID, p.conn.GetSessionID())
	data, err := json.MarshalIndent(*node, "", "  ")
	flags := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)

	// it is possible the live instance still exists from last run
	// retry 5 seconds to wait for the zookeeper to remove the live instance
	// from previous session
	retry := 15

	_, err = p.conn.Create(path, data, flags, acl)

	for retry > 0 && err == zk.ErrNodeExists {
		select {
		case <-time.After(1 * time.Second):
			_, err = p.conn.Create(path, data, flags, acl)
			if err != nil {
				retry--
			}
		}
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

// public enum MessageState {
//   NEW,
//   READ, // not used
//   UNPROCESSABLE // get exception when create handler
// }
func (p *Participant) processMessage(msgID string) {
	fmt.Println("Process message: " + msgID)

	msgPath := p.kb.message(p.ParticipantID, msgID)
	message, err := p.conn.GetRecordFromPath(msgPath)
	must(err)

	msgType := message.GetSimpleField("MSG_TYPE").(string)

	if msgType == "NO_OP" {
		//Logger.Printf("Dropping NO-OP message. mid: %s, from: %s\n", msgID, message.GetSimpleField("SRC_NAME"))
		fmt.Println("Delete NO-OP message: " + msgID)
		p.conn.DeleteTree(msgPath)
		return
	}

	sessionID := message.GetSimpleField("TGT_SESSION_ID").(string)

	// sessionID mismatch normally means message comes from expired session, just remove it
	if sessionID != p.conn.GetSessionID() && sessionID != "*" {
		//Logger.Printf("SessionId does NOT match. Expected sessionId: %s, tgtSessionId in message: %s, messageId: %s\n", p.conn.GetSessionID(), sessionID, msgID)

		fmt.Println("delete expired message: " + msgID + ". expected sessionID:" + p.conn.GetSessionID() + ", tgtSessionID in message:" + sessionID)
		p.conn.DeleteTree(msgPath)
		return
	}

	// don't process message that is of READ or UNPROCESSABLE state
	msgState := message.GetSimpleField("MSG_STATE").(string)
	// ignore the message if it is READ. The READ message is not deleted until the state has changed
	if !strings.EqualFold(msgState, "NEW") {
		fmt.Println("skip message: " + msgID)
		return
	}

	// update msgState to read
	message.SetSimpleField("MSG_STATE", "READ")
	message.SetSimpleField("READ_TIMESTAMP", time.Now().Unix())
	message.SetSimpleField("EXE_SESSION_ID", p.conn.GetSessionID())

	// create current state meta data
	// do it for non-controller and state transition messages only
	targetName := message.GetSimpleField("TGT_NAME").(string)
	if !strings.EqualFold(targetName, "CONTROLLER") && strings.EqualFold(msgType, "STATE_TRANSITION") {
		resourceID := message.GetSimpleField("RESOURCE_NAME").(string)
		currentStateRecord := helix.NewRecord(resourceID)

		bucketSize := message.GetIntField("BUCKET_SIZE", 0)
		currentStateRecord.SetIntField("BUCKET_SIZE", bucketSize)

		stateModelRef := message.GetSimpleField("STATE_MODEL_DEF")
		currentStateRecord.SetSimpleField("STATE_MODEL_DEF", stateModelRef)

		currentStateRecord.SetSimpleField("SESSION_ID", sessionID)

		batchMode := message.GetBooleanField("BATCH_MESSAGE_MODE", false)
		currentStateRecord.SetBooleanField("BATCH_MESSAGE_MODE", batchMode)

		factoryName := message.GetSimpleField("STATE_MODEL_FACTORY_NAME")
		if factoryName != nil {
			currentStateRecord.SetSimpleField("STATE_MODEL_FACTORY_NAME", factoryName)
		} else {
			currentStateRecord.SetSimpleField("STATE_MODEL_FACTORY_NAME", "DEFAULT")
		}

		// save to zookeeper
		path := p.kb.currentStateForResource(p.ParticipantID, sessionID, resourceID)

		// let's only set the current state if it is empty
		if exists, _ := p.conn.Exists(path); !exists {
			fmt.Println("Setting " + path + ":\n" + currentStateRecord.String())
			err := p.conn.SetRecordForPath(path, currentStateRecord)
			must(err)
		}
	}

	p.handleStateTransition(message)

	// after the message is processed successfully, remove it
	p.conn.DeleteTree(msgPath)
}

func (p *Participant) handleStateTransition(message *helix.Record) {
	// verify the fromState with the current state model
	fromState := message.GetSimpleField("FROM_STATE").(string)
	toState := message.GetSimpleField("TO_STATE").(string)

	fmt.Printf("State transition from %s to %s\n", fromState, toState)

	// set the message execution time
	nowMilli := time.Now().UnixNano() / 1000000
	startTime := strconv.FormatInt(nowMilli, 10)
	message.SetSimpleField("EXECUTE_START_TIMESTAMP", startTime)

	p.preHandleMessage(message)
	// TODO: invoke state model transition function

	p.postHandleMessage(message)

}

func (p *Participant) preHandleMessage(message *helix.Record) {

}

func (p *Participant) postHandleMessage(message *helix.Record) {
	// sessionID might change when we update the state model
	// skip if we are handling an expired session
	sessionID := p.conn.GetSessionID()
	targetSessionID := message.GetSimpleField("TGT_SESSION_ID")
	toState := message.GetSimpleField("TO_STATE").(string)
	partitionName := message.GetSimpleField("PARTITION_NAME").(string)

	if targetSessionID != nil && targetSessionID.(string) != sessionID {
		return
	}

	// if the target state is DROPPED, we need to remove the resource key
	// from the current state of the instance because the resource key is dropped.
	// In the state model it will be stayed as OFFLINE, which is OK.

	if strings.ToUpper(toState) == "DROPPED" {
		path := p.kb.currentStatesForSession(p.ParticipantID, sessionID)
		p.conn.RemoveMapFieldKey(path, partitionName)
	}

	// actually set the current state
	resourceID := message.GetSimpleField("RESOURCE_NAME").(string)
	currentStateForResourcePath := p.kb.currentStateForResource(p.ParticipantID, p.conn.GetSessionID(), resourceID)

	err := p.conn.UpdateMapField(currentStateForResourcePath, partitionName, "CURRENT_STATE", toState)
	must(err)
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
			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()
	return snapshots, errors
}
