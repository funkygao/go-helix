package zk

import (
	"strings"
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/go-helix"
)

func TestManager(t *testing.T) {
	// empty host and port is allowed
	mgr, err := newZkHelixManager(testCluster, "", "", testZkSvr, helix.InstanceTypeParticipant)
	assert.Equal(t, nil, err)
	t.Logf("%s", mgr.instanceID)
	assert.Equal(t, true, strings.HasSuffix(mgr.instanceID, "-PARTICIPANT"))

}
