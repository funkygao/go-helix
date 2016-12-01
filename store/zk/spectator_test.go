package zk

import (
	"testing"
	"time"
)

func TestSpectatorConnect(t *testing.T) {
	now := time.Now().Local()
	cluster := "spactator_test_TestSpectatorConnect_" + now.Format("20060102150405")

	a := Admin{zkSvr: testZkSvr}
	a.AddCluster(cluster)
	defer a.DropCluster(cluster)

	listenerCalled := false

	externalViewChangeListener := func(ev []*Record, c *Context) {
		listenerCalled = true
	}

	manager := NewHelixManager(testZkSvr)
	// s := manager.NewSpectator(cluster, externalViewChangeListener, nil)
	s := manager.NewSpectator(cluster)
	s.AddExternalViewChangeListener(externalViewChangeListener)
	s.Connect()
	defer s.Disconnect()

	// create a participant to trigger the external view change
	p := manager.NewParticipant(cluster, "localhost", "12913")

	sm1 := NewStateModel(nil)
	p.RegisterStateModel("dummy", sm1)
	p.Connect()
	defer p.Disconnect()

	select {
	case <-time.After(1 * time.Second):
		if listenerCalled == false {
			t.Error("Failed to call the external view listener")
		}
	}
}
