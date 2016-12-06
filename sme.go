package helix

// Helix participant manager uses StateMachineEngine to register/remove state model transition.
// The state transition handles state transition messages.
type StateMachineEngine interface {

	// RegisterStateModel associates state trasition functions with the participant.
	RegisterStateModel(stateModel string, sm *StateModel) error

	// RemoveStateModel disconnects a state transition with the participant.
	RemoveStateModel(stateModel string) error

	StateModel(stateModel string) (*StateModel, bool)
}
