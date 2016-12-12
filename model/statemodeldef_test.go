package model

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestStateModelDef(t *testing.T) {
	smd := NewStateModelDef("foobar")
	smd.SetInitialState("OFF")
	assert.Equal(t, "OFF", smd.InitialState())
}
