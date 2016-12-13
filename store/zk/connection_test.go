package zk

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/go-helix"
)

func TestConnectionRecordOperation(t *testing.T) {
	c := newConnection(testZkSvr)

	// IsClusterSetup
	yes, err := c.IsClusterSetup("")
	assert.Equal(t, false, yes)
	assert.Equal(t, helix.ErrInvalidArgument, err)
	yes, err = c.IsClusterSetup(testCluster)
	assert.Equal(t, false, yes)
	assert.Equal(t, helix.ErrNotConnected, err)

	yes, err = c.IsInstanceSetup("", "")
	assert.Equal(t, false, yes)
	assert.Equal(t, helix.ErrInvalidArgument, err)
}
