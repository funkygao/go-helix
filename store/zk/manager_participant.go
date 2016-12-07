package zk

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
	"github.com/yichen/go-zookeeper/zk"
)

func (m *Manager) initParticipant() error {
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

	return m.createLiveInstance()
}

func (p *Manager) cleanUpStaleSessions() error {
	log.Trace("P[%s/%s] cleanup stale sessions", p.instanceID, p.conn.GetSessionID())

	sessions, err := p.conn.Children(p.kb.currentStates(p.instanceID))
	if err != nil {
		return err
	}

	for _, sessionID := range sessions {
		if sessionID != p.conn.GetSessionID() {
			log.Warn("P[%s/%s] found stale session: %s", p.instanceID, p.conn.GetSessionID(), sessionID)

			if err = p.conn.DeleteTree(p.kb.currentStatesForSession(p.instanceID, sessionID)); err != nil {
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

// Ensure that ZNodes for a participant all exist.
func (p *Manager) joinCluster() (bool, error) {
	// /{cluster}/CONFIGS/PARTICIPANT/localhost_12000
	exists, err := p.conn.Exists(p.kb.participantConfig(p.instanceID))
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
	participant := model.NewRecord(p.instanceID)
	participant.SetSimpleField("HELIX_HOST", p.Host)
	participant.SetSimpleField("HELIX_PORT", p.Port)
	participant.SetSimpleField("HELIX_ENABLED", "true")
	err = any(
		p.conn.CreateRecordWithPath(p.kb.participantConfig(p.instanceID), participant),

		// /{cluster}/INSTANCES/localhost_12000
		p.conn.CreateEmptyNode(p.kb.instance(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/CURRENTSTATES
		p.conn.CreateEmptyNode(p.kb.currentStates(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/ERRORS
		p.conn.CreateEmptyNode(p.kb.errorsR(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/HEALTHREPORT
		p.conn.CreateEmptyNode(p.kb.healthReport(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/MESSAGES
		p.conn.CreateEmptyNode(p.kb.messages(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/STATUSUPDATES
		p.conn.CreateEmptyNode(p.kb.statusUpdates(p.instanceID)),
	)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (p *Manager) startEventLoop() {
	log.Trace("P[%s/%s] starting main loop", p.instanceID, p.conn.GetSessionID())

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
				log.Error("P[%s/%s] %v", p.instanceID, p.conn.GetSessionID(), err)

			case <-p.stop:
				log.Trace("P[%s/%s] stopped", p.instanceID, p.conn.GetSessionID())
				return
			}
		}
	}()
}

func (p *Manager) watchMessages() (chan []string, chan error) {
	snapshots := make(chan []string)
	errors := make(chan error)

	go func() {
		path := p.kb.messages(p.instanceID)
		for {
			snapshot, events, err := p.conn.ChildrenW(path)
			if err != nil {
				errors <- err
				return
			}

			snapshots <- snapshot
			evt := <-events
			log.Debug("P[%s/%s] recv message: %+v %+v", p.instanceID, p.conn.GetSessionID(), snapshot, evt)

			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()

	return snapshots, errors
}

func (p *Manager) createLiveInstance() error {
	record := model.NewLiveInstanceRecord(p.instanceID, p.conn.GetSessionID())
	data, err := json.MarshalIndent(*record, "", "  ")
	flags := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)

	// it is possible the live instance still exists from last run
	// retry 5 seconds to wait for the zookeeper to remove the live instance
	// from previous session
	for retry := 0; retry < 10; retry++ {
		_, err = p.conn.Create(p.kb.liveInstance(p.instanceID), data, flags, acl)
		if err == nil {
			log.Trace("P[%s/%s] become alive", p.instanceID, p.conn.GetSessionID())
			return nil
		}

		// wait for zookeeper remove the last run's ephemeral znode
		time.Sleep(time.Second * 5)

	}

	return err
}

// handleClusterMessage dispatches the cluster message to the corresponding
// handler in the state model.
func (p *Manager) processMessage(msgID string) error {
	log.Debug("P[%s/%s] processing msg: %s", p.instanceID, p.conn.GetSessionID(), msgID)

	msgPath := p.kb.message(p.instanceID, msgID)
	record, err := p.conn.GetRecordFromPath(msgPath)
	if err != nil {
		return err
	}

	message := model.NewMessageFromRecord(record)

	log.Debug("P[%s/%s] message: %s", p.instanceID, p.conn.GetSessionID(), message)

	msgType := message.MessageType()
	if msgType == helix.MessageTypeNoOp {
		log.Warn("P[%s/%s] discard NO-OP message: %s", p.instanceID, p.conn.GetSessionID(), msgID)
		return p.conn.DeleteTree(msgPath)
	}

	sessionID := message.TargetSessionID()
	if sessionID != "*" && sessionID != p.conn.GetSessionID() {
		// message comes from expired session
		log.Warn("P[%s/%s] got mismatched message: %s", p.instanceID, p.conn.GetSessionID(), message)
		return p.conn.DeleteTree(msgPath)
	}

	if !strings.EqualFold(message.MessageState(), helix.MessageStateNew) {
		// READ message is not deleted until the state has changed
		log.Warn("P[%s/%s] skip %s message: %s", p.instanceID, p.conn.GetSessionID(), message.MessageState(), msgID)
		return nil
	}

	// update msgState to read
	message.SetSimpleField("MSG_STATE", helix.MessageStateRead)
	message.SetSimpleField("READ_TIMESTAMP", time.Now().Unix())
	message.SetSimpleField("EXE_SESSION_ID", p.conn.GetSessionID())

	// create current state meta data
	// do it for non-controller and state transition messages only
	targetName := message.TargetName()
	if !strings.EqualFold(targetName, string(helix.InstanceTypeController)) && strings.EqualFold(msgType, helix.MessageTypeStateTransition) {
		resourceID := message.Resource()

		currentStateRecord := model.NewRecord(resourceID)
		currentStateRecord.SetIntField("BUCKET_SIZE", message.GetIntField("BUCKET_SIZE", 0))
		currentStateRecord.SetSimpleField("STATE_MODEL_DEF", message.GetSimpleField("STATE_MODEL_DEF"))
		currentStateRecord.SetSimpleField("SESSION_ID", sessionID)
		currentStateRecord.SetBooleanField("BATCH_MESSAGE_MODE", message.GetBooleanField("BATCH_MESSAGE_MODE", false))
		currentStateRecord.SetSimpleField("STATE_MODEL_FACTORY_NAME", message.GetStringField("STATE_MODEL_FACTORY_NAME", "DEFAULT"))

		resourceCurrentStatePath := p.kb.currentStateForResource(p.instanceID, sessionID, resourceID)
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
	log.Trace("P[%s/%s] state %s -> %s", p.instanceID,
		p.conn.GetSessionID(), message.FromState(), message.ToState())

	// set the message execution time
	nowMilli := time.Now().UnixNano() / 1000000
	startTime := strconv.FormatInt(nowMilli, 10)
	message.SetSimpleField("EXECUTE_START_TIMESTAMP", startTime)

	p.preHandleMessage(message)

	// TODO lock
	transition, present := p.sme.StateModel(message.StateModelDef())
	if !present {
		log.Error("P[%s/%s] has no transition defined for state model %s", p.instanceID,
			p.conn.GetSessionID(), message.StateModelDef())
	} else {
		handler := transition.Handler(message.FromState(), message.ToState())
		if handler == nil {
			log.Warn("P[%s/%s] state %s -> %s has no handler", p.instanceID,
				p.conn.GetSessionID(), message.FromState(), message.ToState())
		} else {
			context := helix.NewContext(p)
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
		p.conn.RemoveMapFieldKey(p.kb.currentStatesForSession(p.instanceID, sessionID), partitionName)
	}

	// actually set the current state
	currentStateForResourcePath := p.kb.currentStateForResource(p.instanceID,
		p.conn.GetSessionID(), message.Resource())
	return p.conn.UpdateMapField(currentStateForResourcePath, partitionName,
		"CURRENT_STATE", toState)
}
