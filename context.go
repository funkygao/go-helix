package helix

import "sync"

// Context is a goroutine safe property bag for storing data between listeners and callbacks.
type Context struct {
	sync.RWMutex
	data map[string]interface{}

	Manager HelixManager
}

// NewContext creates a new Context instance.
func NewContext() *Context {
	return &Context{
		data: make(map[string]interface{}),
	}
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
