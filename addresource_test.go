package helix

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestDefaultAddResourceOption(t *testing.T) {
	opt := DefaultAddResourceOption(3, StateModelLeaderStandby)
	assert.Equal(t, 3, opt.Partitions)
	assert.Equal(t, RebalancerModeSemiAuto, opt.RebalancerMode)
}

func TestAddResourceOptionValidate(t *testing.T) {
	opt := DefaultAddResourceOption(3, StateModelMasterSlave)
	assert.Equal(t, true, opt.Valid())

	opt.RebalancerMode = ""
	assert.Equal(t, false, opt.Valid())
}
