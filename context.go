package helix

import "sync"

// Context is a goroutine safe property bag for storing data between listeners and callbacks.
type Context struct {
	sync.RWMutex
	data map[string]interface{}

	manager HelixManager
}

// NewContext creates a new Context instance.
func NewContext(m HelixManager) *Context {
	return &Context{
		data:    make(map[string]interface{}),
		manager: m,
	}
}

func (c Context) Manager() HelixManager {
	return c.manager
}

// Set sets a key value pair.
func (c *Context) Set(key string, value interface{}) {
	c.Lock()
	c.data[key] = value
	c.Unlock()
}

// Get gets the value of a key.
func (c *Context) Get(key string) interface{} {
	v, ok := c.data[key]
	if !ok {
		return nil
	}

	return v
}
