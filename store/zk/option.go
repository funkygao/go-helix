package zk

type managerOption func(*Manager)

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
