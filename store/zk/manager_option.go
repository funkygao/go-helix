package zk

import (
	"time"
)

type ManagerOption func(*Manager)

func WithoutPprof() ManagerOption {
	return func(m *Manager) {
		m.pprofPort = 0
	}
}

func WithPprofPort(port int) ManagerOption {
	return func(m *Manager) {
		m.pprofPort = port
	}
}

func WithParticipantID(id string) ManagerOption {
	return func(m *Manager) {
		m.instanceID = id
	}
}

func WithZkSessionTimeout(d time.Duration) ManagerOption {
	return func(m *Manager) {
		m.conn.SetSessionTimeout(d)
	}
}
