package zk

import (
	"time"
)

type managerOption func(*Manager)

type zkConnOption func(*connection)

func WithManagerZkSessionTimeout(sessionTimeout time.Duration) managerOption {
	return func(m *Manager) {
		m.conn.sessionTimeout = sessionTimeout
	}
}

func WithoutPprof() managerOption {
	return func(m *Manager) {
		m.pprofPort = 0
	}
}

func WithPprofPort(port int) managerOption {
	return func(m *Manager) {
		m.pprofPort = port
	}
}

func WithParticipantID(id string) managerOption {
	return func(m *Manager) {
		m.instanceID = id
	}
}

func WithZkSessionTimeout(sessionTimeout time.Duration) zkConnOption {
	return func(c *connection) {
		c.sessionTimeout = sessionTimeout
	}
}
