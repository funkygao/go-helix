package helix

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/funkygao/go-helix/model"
)

// Transition associates a handler function with the state transition from the from state
// to the to state.
type Transition struct {
	FromState string
	ToState   string
	Handler   func(*model.Message, *Context)
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
	handler func(*model.Message, *Context)) error {
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

func (sm *StateModel) Handler(fromState, toState string) func(*model.Message, *Context) {
	for _, tr := range sm.transitions {
		if tr.FromState == fromState && tr.ToState == toState {
			return tr.Handler
		}
	}

	return nil
}

// ExportDiagram exports the state model transitions to a diagram.
func (sm *StateModel) ExportDiagram(outfile string) error {
	return sm.exportStateDiagram(outfile, "png", "dot", "72", "-Gsize=10,5 -Gdpi=200")
}

func (sm *StateModel) exportStateDiagram(outfile string, format string, layout string, scale string, more string) error {
	dot := `digraph StateMachine {
	rankdir=LR
	node[width=1 fixedsize=true shape=circle style=filled fillcolor="darkorchid1" ]
	
	`

	for _, t := range sm.transitions {
		link := fmt.Sprintf(`%s -> %s`, t.FromState, t.ToState)
		dot = dot + "\r\n" + link
	}

	dot = dot + "\r\n}"
	cmdLine := fmt.Sprintf("dot -o%s -T%s -K%s -s%s %s", outfile, format, layout, scale, more)

	cmd := exec.Command(`/bin/sh`, `-c`, cmdLine)
	cmd.Stdin = strings.NewReader(dot)

	return cmd.Run()
}
