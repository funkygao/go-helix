package zk

import (
	"time"
)

type zkConnOption func(*connection)

func WithSessionTimeout(sessionTimeout time.Duration) zkConnOption {
	return func(c *connection) {
		c.sessionTimeout = sessionTimeout
	}
}
