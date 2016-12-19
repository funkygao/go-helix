package zk

import (
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
)

type transitionMessageHandler struct {
	*Manager
	message *model.Message
}

func newTransitionMessageHandler(mgr *Manager, message *model.Message) *transitionMessageHandler {
	return &transitionMessageHandler{
		Manager: mgr,
		message: message,
	}
}

func (h *transitionMessageHandler) preHandleMessage() error {
	return nil
}

func (p *transitionMessageHandler) postHandleMessage() error {
	// sessionID might change when we update the state model
	// skip if we are handling an expired session
	message := p.message

	sessionID := p.conn.SessionID()
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
		p.conn.SessionID(), message.Resource())
	return p.conn.UpdateMapField(currentStateForResourcePath, partitionName,
		"CURRENT_STATE", toState)
}

func (p *transitionMessageHandler) invoke() {
	message := p.message

	// TODO lock
	transition, present := p.sme.StateModel(message.StateModelDef())
	if !present {
		log.Error("P[%s/%s] has no transition defined for state model %s", p.instanceID,
			p.conn.SessionID(), message.StateModelDef())
	} else {
		handler := transition.Handler(message.FromState(), message.ToState())
		if handler == nil {
			log.Warn("P[%s/%s] state %s -> %s has no handler", p.instanceID,
				p.conn.SessionID(), message.FromState(), message.ToState())
		} else {
			context := helix.NewContext(p)
			handler(message, context)
		}
	}
}

func (p *transitionMessageHandler) handleMessage() {
	message := p.message

	log.Trace("%s state %s -> %s", p.shortID(), message.FromState(), message.ToState())

	// set the message execution time
	nowMilli := time.Now().UnixNano() / 1000000
	startTime := strconv.FormatInt(nowMilli, 10)
	message.SetSimpleField("EXECUTE_START_TIMESTAMP", startTime)

	p.preHandleMessage()

	p.invoke()
	p.postHandleMessage()
}
