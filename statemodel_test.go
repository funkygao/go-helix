package helix

import (
	"testing"

	"github.com/funkygao/go-helix/model"
)

func TestNewStateModel(t *testing.T) {
	t.Parallel()

	sm1 := NewStateModel()

	if len(sm1.transitions) != 0 {
		t.Error("The Statemodel should be empty")
	}

	fromOfflineToOnline := func(*model.Message, *Context) {}
	fromOnlineToOffline := func(*model.Message, *Context) {}

	sm2 := NewStateModel()
	sm2.AddTransitions([]Transition{
		{"OFFLINE", "ONLINE", fromOfflineToOnline},
	})

	if len(sm2.transitions) != 1 {
		t.Error("The StateModel.Size() should reeturn 1")
	}

	sm2.AddTransition("ONLINE", "OFFLINE", fromOnlineToOffline)
	if len(sm2.transitions) != 2 {
		t.Error("The StateModel.Size() should reeturn 2")
	}
}
