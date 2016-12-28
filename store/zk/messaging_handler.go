package zk

import (
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
)

var _ helix.MessageHandler = &transitionMessageHandler{}

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

func (h *transitionMessageHandler) HandleMessage(message *model.Message) error {
	log.Debug("msg[%s] %s, %s -> %s", message.ID(),
		message.PartitionName(),
		message.FromState(), message.ToState())

	if err := h.preHandleMessage(message); err != nil {
		return err
	}

	h.invoke(message)

	return h.postHandleMessage(message)
}

func (h *transitionMessageHandler) preHandleMessage(message *model.Message) error {
	log.Debug("msg[%s] pre handle", message.ID())

	if !message.Valid() {
		return helix.ErrInvalidMessage
	}

	// set REQUESTED_STATE of current state delta list

	// set the message execution time
	nowMilli := time.Now().UnixNano() / 1000000
	startTime := strconv.FormatInt(nowMilli, 10)
	message.SetSimpleField("EXECUTE_START_TIMESTAMP", startTime)

	return nil
}

func (h *transitionMessageHandler) postHandleMessage(message *model.Message) error {
	log.Debug("msg[%s] post handle", message.ID())

	// sessionID might change when we update the state model
	// skip if we are handling an expired session

	sessionID := h.conn.SessionID()
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
		h.conn.RemoveMapFieldKey(h.kb.currentStatesForSession(h.instanceID, sessionID), partitionName)
	}

	log.Debug("msg[%s] dropped", message.ID())
	h.conn.DeleteTree(h.kb.message(h.instanceID, message.ID()))

	// actually set the current state
	currentStateForResourcePath := h.kb.currentStateForResource(h.instanceID,
		h.conn.SessionID(), message.Resource())
	log.Debug("msg[%s] %s[CURRENT_STATE] => %s", message.ID(), currentStateForResourcePath, toState)
	return h.conn.UpdateMapField(currentStateForResourcePath, partitionName,
		"CURRENT_STATE", toState)
}

func (h *transitionMessageHandler) invoke(message *model.Message) {
	log.Debug("msg[%s] invoke", message.ID())

	// TODO lock
	transition, present := h.sme.StateModel(message.StateModelDef())
	if !present {
		log.Error("%s has no transition defined for state model %s", h.shortID(), message.StateModelDef())
	} else {
		if handler := transition.Handler(message.FromState(), message.ToState()); handler == nil {
			log.Warn("msg[%s] %s %s -> %s empty handler", message.ID(), message.Resource(), message.FromState(), message.ToState())
		} else {
			context := helix.NewContext(h)
			handler(message, context)
		}
	}
}
