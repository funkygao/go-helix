package zk

import (
	"strings"
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/go-helix"
)

func TestManagerBasic(t *testing.T) {
	// empty host and port is allowed
	mgr, err := newZkHelixManager(testCluster, "", "", testZkSvr, helix.InstanceTypeParticipant)
	assert.Equal(t, nil, err)
	t.Logf("%s", mgr.instanceID)
	assert.Equal(t, true, strings.HasSuffix(mgr.Instance(), "-PARTICIPANT"))
	assert.Equal(t, testCluster, mgr.Cluster())
	assert.Equal(t, false, mgr.IsLeader())
	assert.Equal(t, helix.InstanceTypeParticipant, mgr.InstanceType())
	assert.Equal(t, nil, mgr.Connect())
	assert.Equal(t, true, mgr.IsConnected())
	assert.Equal(t, true, len(mgr.SessionID()) > 3)
	assert.Equal(t, true, mgr.ClusterManagementTool() != nil)
	assert.Equal(t, true, mgr.MessagingService() != nil)
	mgr.Disconnect()
}

func TestManagerConnectAndDisconnect(t *testing.T) {
	mgr, err := newZkHelixManager("invalid-cluster", "", "", testZkSvr, helix.InstanceTypeParticipant)
	assert.Equal(t, nil, err)
	assert.Equal(t, helix.ErrClusterNotSetup, mgr.Connect())

	mgr, err = newZkHelixManager(testCluster, "", "", testZkSvr, helix.InstanceTypeParticipant)
	assert.Equal(t, nil, err)
	assert.Equal(t, nil, mgr.Connect())
	mgr.Disconnect()
}
