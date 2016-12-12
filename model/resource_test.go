package model

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestResource(t *testing.T) {
	r := NewResource("redis")
	r.AddPartition("0").AddPartition("2").AddPartition("1")
	assert.Equal(t, 3, len(r.Partitions()))
}
