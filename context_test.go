package helix

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestContextGetSet(t *testing.T) {
	c := NewContext(nil)
	c.Set("a", 1)
	assert.Equal(t, 1, c.Get("a").(int))

}

func TestContextSetNX(t *testing.T) {
	c := NewContext(nil)
	assert.Equal(t, true, c.SetNX("a", 1))
	assert.Equal(t, false, c.SetNX("a", 1))
	assert.Equal(t, 1, c.Get("a").(int))
}
