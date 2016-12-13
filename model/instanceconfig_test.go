package model

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestInstanceConfig(t *testing.T) {
	r := NewRecord("id")
	ic := NewInstanceConfigFromRecord(r)
	ic.SetHost("host")
	assert.Equal(t, "host", ic.Host())
	ic.SetPort("10009")
	assert.Equal(t, "10009", ic.Port())

	// node=host_port
	assert.Equal(t, "host_10009", ic.Node())
	assert.Equal(t, "host_10009", ic.InstanceName())

	// enabled by default
	assert.Equal(t, true, ic.IsEnabled())

	ic.Enable(false)
	assert.Equal(t, false, ic.IsEnabled())

	// validate
	assert.Equal(t, nil, ic.Validate())

	// tag
	ic.AddTag("tag1")
	assert.Equal(t, 1, len(ic.Tags()))
	ic.AddTag("tag2")
	assert.Equal(t, 2, len(ic.Tags()))
	assert.Equal(t, "tag2", ic.Tags()[1])
}
