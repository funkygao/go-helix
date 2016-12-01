package zk

import (
	"testing"
	"time"

	"github.com/funkygao/go-helix"
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

	manager := NewZKHelixManager(testZkSvr)
	p := manager.NewParticipant(cluster, "localhost", "12913")

	if p == nil {
		t.Error("NewParticipant should return valid object, not nil")
	}

	sm1 := helix.NewStateModel(nil)
	p.RegisterStateModel("dummy", sm1)

	err := p.Start()
	if err != helix.ErrEnsureParticipantConfig {
		// expect to fail with ErrEnsureParticipantConfig
		t.Error("expect to fail with ErrEnsureParticipantConfig")
	}

	// expect the connect to fail because the auto join is not allowed
	pp := p.(*Participant)
	if pp.state == psStarted {
		t.Error("Expect the participant to fail to connect")
	}

	// now enable allowPartitipantAutoJoin
	property := map[string]string{
		"allowParticipantAutoJoin": "true",
	}

	a.SetConfig(cluster, "CLUSTER", property)

	// and try connect again
	err = p.Start()
	if err != nil {
		t.Error(err)
	}
	defer p.Close()
	if pp.state != psStarted {
		t.Error("Participant is not connected and started")
	}
}
