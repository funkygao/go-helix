package helix

// Transition associates a handler function with the state transition from the from state
// to the to state.
type Transition struct {
	FromState string
	ToState   string
	Handler   func(*Message, *Context)
}

// StateModel is a collection of state transitions and their handlers.
type StateModel struct {
	transitions []Transition
}

// NewStateModel creates an empty state model.
func NewStateModel() *StateModel {
	return &StateModel{
		transitions: make([]Transition, 0),
	}
}

// AddTransition add a state transition handler to the state model.
func (sm *StateModel) AddTransition(fromState string, toState string,
	handler func(*Message, *Context)) error {
	// validate
	for _, tr := range sm.transitions {
		if fromState == tr.FromState && toState == tr.ToState {
			return ErrDupStateTransition
		}
	}

	transition := Transition{fromState, toState, handler}
	sm.transitions = append(sm.transitions, transition)
	return nil
}

// AddTransition add a state transition handler to the state model.
func (sm *StateModel) AddTransitions(transitions []Transition) error {
	for _, tr := range transitions {
		if err := sm.AddTransition(tr.FromState, tr.ToState, tr.Handler); err != nil {
			return err
		}
	}

	return nil
}

func (sm *StateModel) Handler(fromState, toState string) func(*Message, *Context) {
	for _, tr := range sm.transitions {
		if tr.FromState == fromState && tr.ToState == toState {
			return tr.Handler
		}
	}

	return nil
}
