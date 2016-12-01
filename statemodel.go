package helix

// Transition associates a handler function with the state transition from the from state
// to the to state.
type Transition struct {
	FromState string
	ToState   string
	Handler   func(string)
}

// StateModel is a collection of state transitions and their handlers
type StateModel struct {
	transitions []Transition
}

// NewStateModel creates an empty state model
func NewStateModel(transitions []Transition) StateModel {
	return StateModel{transitions}
}

// Size is the number of transitions in the state model
func (sm *StateModel) Size() int {
	return len(sm.transitions)
}

// AddTransition add a state transition handler to the state model
func (sm *StateModel) AddTransition(fromState string, toState string, handler func(string)) {
	transition := Transition{fromState, toState, handler}
	// TODO validate
	sm.transitions = append(sm.transitions, transition)
}
