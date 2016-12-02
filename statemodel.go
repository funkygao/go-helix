package helix

// Transition associates a handler function with the state transition from the from state
// to the to state.
type Transition struct {
	FromState string
	ToState   string
	Handler   func(partition string)
}

// StateModel is a collection of state transitions and their handlers.
type StateModel struct {
	transitions []Transition
}

// NewStateModel creates an empty state model.
func NewStateModel(transitions []Transition) StateModel {
	return StateModel{transitions}
}

// AddTransition add a state transition handler to the state model.
// TODO validate
func (sm *StateModel) AddTransition(fromState string, toState string, handler func(string)) {
	transition := Transition{fromState, toState, handler}
	sm.transitions = append(sm.transitions, transition)
}

func (sm *StateModel) Handler(fromState, toState string) func(string) {
	for _, tr := range sm.transitions {
		if tr.FromState == fromState && tr.ToState == toState {
			return tr.Handler
		}
	}

	return nil
}
