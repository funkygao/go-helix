package zk

import (
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
)

var _ helix.ClusterMessagingService = &zkMessagingService{}

// TODO support other types of message besides STATE_TRANSITION
type zkMessagingService struct {
	*Manager
}

func newZkMessagingService(m *Manager) *zkMessagingService {
	return &zkMessagingService{
		Manager: m,
	}
}

func (m *zkMessagingService) Send(msg *model.Message) error {
	return nil
}

func (p *zkMessagingService) onConnected() {
	log.Trace("%s starting messaging main loop...", p.shortID())

	messagesChan, errChan := p.watchMessages()
	go func() {
		for {
			select {
			case m := <-messagesChan:
				for _, msg := range m {
					// messageChan is a snapshot of all unprocessed messages whenever
					// a new message is added, so it will have duplicates.
					if err := p.processMessage(msg); err != nil {
						log.Error("%s %v", p.shortID(), err)
					}
				}

			case err := <-errChan:
				log.Error("%s %v", p.shortID(), err)

			case <-p.stop:
				log.Trace("%s stopped", p.shortID())
				return
			}
		}
	}()

	log.Trace("%s messaging loop started", p.shortID())
}

func (p *zkMessagingService) watchMessages() (chan []string, chan error) {
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
			log.Debug("%s recv message: %+v %+v", p.shortID(), snapshot, evt)

			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()

	return snapshots, errors
}

// handleClusterMessage dispatches the cluster message to the corresponding
// handler in the state model.
// HelixTaskExecutor.onMessage
func (p *zkMessagingService) processMessage(msgID string) error {
	log.Debug("P[%s/%s] processing msg: %s", p.instanceID, p.conn.GetSessionID(), msgID)

	msgPath := p.kb.message(p.instanceID, msgID)
	record, err := p.conn.GetRecordFromPath(msgPath)
	if err != nil {
		return err
	}

	message := model.NewMessageFromRecord(record)

	log.Debug("%s message: %s", p.shortID(), message)

	msgType := message.MessageType()
	if msgType == helix.MessageTypeNoOp {
		log.Warn("%s discard NO-OP message: %s", p.shortID(), msgID)
		return p.conn.DeleteTree(msgPath)
	}

	sessionID := message.TargetSessionID()
	if sessionID != "*" && sessionID != p.conn.GetSessionID() {
		// message comes from expired session
		log.Warn("%s got mismatched message: %s/%s, dropped", p.shortID(), msgID, sessionID)
		return p.conn.DeleteTree(msgPath)
	}

	if !strings.EqualFold(message.MessageState(), helix.MessageStateNew) {
		// READ message is not deleted until the state has changed
		log.Warn("%s skip %s message: %s", p.shortID(), message.MessageState(), msgID)
		return nil
	}

	// update msgState to read
	message.SetMessageState(helix.MessageStateRead)
	message.SetReadTimestamp(time.Now().Unix())
	message.SetExecuteSessionID(p.conn.GetSessionID())

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

func (p *zkMessagingService) handleStateTransition(message *model.Message) {
	log.Trace("%s state %s -> %s", p.shortID(), message.FromState(), message.ToState())

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

func (p *zkMessagingService) preHandleMessage(message *model.Message) {

}

func (p *zkMessagingService) postHandleMessage(message *model.Message) error {
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
