package helix

import (
	"github.com/funkygao/go-helix/model"
)

// Helix participant manager uses StateMachineEngine to register/remove state model transition.
// The state transition handles state transition messages.
type StateMachineEngine interface {

	// RegisterStateModel associates state trasition functions with the participant.
	RegisterStateModel(stateModelDef string, sm *StateModel) error

	// RemoveStateModel disconnects a state transition with the participant.
	RemoveStateModel(stateModelDef string) error

	// StateModel returns a state model by name.
	StateModel(stateModelDef string) (*StateModel, bool)

	// CreateMessageHandler will create a handler to handle the message.
	CreateMessageHandler(message *model.Message, ctx *ChangeNotification) MessageHandler

	// MessageType returns the message type this state machine engine is interested in.
	MessageType() string

	// Reset will reset the state of the engine.
	Reset()
}
