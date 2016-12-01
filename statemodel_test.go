package helix

import (
	"testing"
)

func TestNewStateModel(t *testing.T) {
	t.Parallel()

	sm1 := NewStateModel(nil)

	if sm1.Size() != 0 {
		t.Error("The Statemodel should be empty")
	}

	fromOfflineToOnline := func(partition string) {}
	fromOnlineToOffline := func(partition string) {}

	sm2 := NewStateModel([]Transition{
		{"OFFLINE", "ONLINE", fromOfflineToOnline},
	})

	if sm2.Size() != 1 {
		t.Error("The StateModel.Size() should reeturn 1")
	}

	sm2.AddTransition("ONLINE", "OFFLINE", fromOnlineToOffline)
	if sm2.Size() != 2 {
		t.Error("The StateModel.Size() should reeturn 2")
	}
}
