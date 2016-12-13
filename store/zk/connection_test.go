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

	// test ErrNotConnected
	yes, err = c.IsClusterSetup(testCluster)
	assert.Equal(t, false, yes)
	assert.Equal(t, helix.ErrNotConnected, err)

	// IsInstanceSetup, invalid arg
	yes, err = c.IsInstanceSetup("", "")
	assert.Equal(t, false, yes)
	assert.Equal(t, helix.ErrInvalidArgument, err)

	// test ErrNotConnected
	yes, err = c.IsInstanceSetup("cluster", "node")
	assert.Equal(t, false, yes)
	assert.Equal(t, helix.ErrNotConnected, err)

	assert.Equal(t, nil, c.Connect())

	// test WaitUntilConnected
	assert.Equal(t, nil, c.WaitUntilConnected(0))

	yes, err = c.IsInstanceSetup("cluster", "node")
	assert.Equal(t, false, yes)
	assert.Equal(t, nil, err)

}

func TestConnectionGetRecord(t *testing.T) {
	c := newConnection(testZkSvr)
	assert.Equal(t, nil, c.Connect())

	t.SkipNow()
}

func TestConnectMapField(t *testing.T) {
	c := newConnection(testZkSvr)
	assert.Equal(t, nil, c.Connect())

	t.SkipNow()
}
