package model

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestIdealState(t *testing.T) {
	r := NewRecord("id")
	i := NewIdealStateFromRecord(r)

	// resource
	assert.Equal(t, "id", i.Resource())

	// rebalance mode
	i.SetRebalanceMode("CUSTOMIZED")
	assert.Equal(t, "CUSTOMIZED", i.RebalanceMode())
}
