package zk

import (
	"testing"
	"time"

	"github.com/funkygao/go-helix"
)

func TestSpectatorConnect(t *testing.T) {
	now := time.Now().Local()
	cluster := "spactator_test_TestSpectatorConnect_" + now.Format("20060102150405")

	a := Admin{zkSvr: testZkSvr}
	a.AddCluster(cluster)
	defer a.DropCluster(cluster)

	listenerCalled := false

	externalViewChangeListener := func(ev []*helix.Record, c *helix.Context) {
		listenerCalled = true
	}

	manager := NewZKHelixManager(testZkSvr)
	// s := manager.NewSpectator(cluster, externalViewChangeListener, nil)
	s := manager.NewSpectator(cluster)
	s.AddExternalViewChangeListener(externalViewChangeListener)
	s.Start()
	defer s.Close()

	// create a participant to trigger the external view change
	p := manager.NewParticipant(cluster, "localhost", "12913")

	sm1 := helix.NewStateModel(nil)
	p.RegisterStateModel("dummy", sm1)
	p.Start()
	defer p.Close()

	select {
	case <-time.After(1 * time.Second):
		if listenerCalled == false {
			t.Error("Failed to call the external view listener")
		}
	}
}
