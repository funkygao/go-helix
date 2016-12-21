package zk

import (
	"strings"
	"sync"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
)

var _ helix.StateMachineEngine = &stateMachineEngine{}

type stateMachineEngine struct {
	sync.RWMutex
	m *Manager

	// all registered state model callbacks
	stateModels map[string]*helix.StateModel
}

func newStateMachineEngine(m *Manager) *stateMachineEngine {
	return &stateMachineEngine{
		m:           m,
		stateModels: make(map[string]*helix.StateModel),
	}
}

func (sme *stateMachineEngine) RegisterStateModel(stateModelDef string, sm *helix.StateModel) error {
	sme.Lock()
	defer sme.Unlock()

	if _, present := sme.stateModels[stateModelDef]; present {
		return helix.ErrDupStateModelName
	}

	sme.stateModels[stateModelDef] = sm
	return nil
}

func (sme *stateMachineEngine) RemoveStateModel(stateModelDef string) error {
	sme.Lock()
	defer sme.Unlock()

	delete(sme.stateModels, stateModelDef)
	return nil
}

func (sme *stateMachineEngine) StateModel(stateModel string) (*helix.StateModel, bool) {
	sme.RLock()
	defer sme.RUnlock()

	r, present := sme.stateModels[stateModel]
	return r, present
}

func (sme *stateMachineEngine) CreateMessageHandler(message *model.Message, ctx *helix.ChangeNotification) helix.MessageHandler {
	if message.MessageType() != sme.MessageType() {
		// should never happen
		log.Critical("ignored %+v", message)
		return nil
	}

	if _, present := sme.StateModel(message.StateModelDef()); !present {
		log.Critical("ignored %+v", message)
		return nil
	}

	msgID := message.ID()
	msgPath := sme.m.kb.message(sme.m.instanceID, msgID)
	targetSessionID := message.TargetSessionID()
	if targetSessionID != "*" && targetSessionID != sme.m.conn.SessionID() {
		// message comes from expired session
		log.Warn("%s got mismatched message: %s/%s, dropped", sme.m.shortID(), msgID, targetSessionID)
		sme.m.conn.DeleteTree(msgPath)
		return nil
	}

	if !strings.EqualFold(message.MessageState(), helix.MessageStateNew) {
		// READ message is not deleted until the state has changed
		log.Warn("%s skip %s message: %s", sme.m.shortID(), message.MessageState(), msgID)
		return nil
	}

	// update msgState to read
	message.SetMessageState(helix.MessageStateRead)
	message.SetReadTimestamp(time.Now().Unix())
	message.SetExecuteSessionID(sme.m.conn.SessionID())

	// create current state meta data
	// do it for non-controller and state transition messages only
	targetName := message.TargetName()
	if !strings.EqualFold(targetName, string(helix.InstanceTypeControllerStandalone)) &&
		strings.EqualFold(message.MessageType(), helix.MessageTypeStateTransition) {
		// FIXME
		resourceID := message.Resource()

		currentStateRecord := model.NewRecord(resourceID)
		currentStateRecord.SetBucketSize(message.BucketSize())
		currentStateRecord.SetBatchMessageMode(message.BatchMessageMode())
		currentStateRecord.SetSimpleField("STATE_MODEL_DEF", message.GetSimpleField("STATE_MODEL_DEF"))
		currentStateRecord.SetSimpleField("SESSION_ID", targetSessionID)
		currentStateRecord.SetSimpleField("STATE_MODEL_FACTORY_NAME", message.GetStringField("STATE_MODEL_FACTORY_NAME", "DEFAULT"))

		resourceCurrentStatePath := sme.m.kb.currentStateForResource(sme.m.instanceID, targetSessionID, resourceID)
		exists, err := sme.m.conn.Exists(resourceCurrentStatePath)
		if err != nil {
			log.Error(err)
			return nil
		}

		if !exists {
			// only set the current state to zookeeper when it is not present
			if err = sme.m.conn.SetRecord(resourceCurrentStatePath, currentStateRecord); err != nil {
				log.Error(err)
				return nil
			}
		}
	}

	return newTransitionMessageHandler(sme.m, message)
}

func (sme *stateMachineEngine) MessageType() string {
	return helix.MessageTypeStateTransition
}

func (sme *stateMachineEngine) Reset() {
	// TODO
}
