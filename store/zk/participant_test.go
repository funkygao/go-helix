package zk

import (
	"testing"
	"time"
)

// TestParticipantConnect makes sure the Participant.Connect
// and Participant.Disconnect work as expected.
func TestParticipantConnect(t *testing.T) {
	t.Parallel()

	now := time.Now().Local()
	cluster := "participant_test_TestParticipantConnect_" + now.Format("20060102150405")

	a := Admin{zkSvr: testZkSvr}
	a.AddCluster(cluster)
	defer a.DropCluster(cluster)

	manager := NewHelixManager(testZkSvr)
	p := manager.NewParticipant(cluster, "localhost", "12913")

	if p == nil {
		t.Error("NewParticipant should return valid object, not nil")
	}

	sm1 := NewStateModel(nil)
	p.RegisterStateModel("dummy", sm1)

	err := p.Connect()
	if err != ErrEnsureParticipantConfig {
		// expect to fail with ErrEnsureParticipantConfig
		t.Error("expect to fail with ErrEnsureParticipantConfig")
	}

	// expect the connect to fail because the auto join is not allowed
	if p.state == psStarted {
		t.Error("Expect the participant to fail to connect")
	}

	// now enable allowPartitipantAutoJoin
	property := map[string]string{
		"allowParticipantAutoJoin": "true",
	}

	a.SetConfig(cluster, "CLUSTER", property)

	// and try connect again
	err = p.Connect()
	if err != nil {
		t.Error(err)
	}
	defer p.Disconnect()
	if p.state != psStarted {
		t.Error("Participant is not connected and started")
	}
}

func TestPreConnectedCallback(t *testing.T) {
	t.Parallel()

	now := time.Now().Local()
	cluster := "participant_test_TestPreConnectedCallback_" + now.Format("20060102150405")

	a := Admin{zkSvr: testZkSvr}
	a.AddCluster(cluster)
	defer a.DropCluster(cluster)

	manager := NewHelixManager(testZkSvr)
	p := manager.NewParticipant(cluster, "localhost", "12913")

	sm1 := NewStateModel(nil)
	p.RegisterStateModel("dummy", sm1)

	called := false
	cb := func() {
		called = true
	}

	// now enable allowPartitipantAutoJoin
	property := map[string]string{
		"allowParticipantAutoJoin": "true",
	}

	a.SetConfig(cluster, "CLUSTER", property)

	p.AddPreConnectCallback(cb)

	err := p.Connect()
	if err != nil {
		t.Error(err)
	}
	p.Disconnect()

	if called != true {
		t.Error("preConnect callback failed to be called")
	}

}
