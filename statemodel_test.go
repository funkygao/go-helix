package helix

import (
	"testing"
)

func TestNewStateModel(t *testing.T) {
	t.Parallel()

	sm1 := NewStateModel(nil)

	if len(sm1.transitions) != 0 {
		t.Error("The Statemodel should be empty")
	}

	fromOfflineToOnline := func(partition string) {}
	fromOnlineToOffline := func(partition string) {}

	sm2 := NewStateModel([]Transition{
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
