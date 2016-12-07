package zk

import (
	"github.com/funkygao/go-helix"
)

func (m *Manager) AddPreConnectCallback(cb helix.PreConnectCallback) {
	m.preConnectCallbacks = append(m.preConnectCallbacks, cb)
}

func (m *Manager) Cluster() string {
	return m.clusterID
}

func (m *Manager) InstanceType() helix.InstanceType {
	return m.it
}

func (m *Manager) Instance() string {
	return m.instanceID
}

func (m *Manager) SessionID() string {
	return m.conn.GetSessionID()
}

func (m *Manager) StateMachineEngine() helix.StateMachineEngine {
	return m.sme
}

// TODO kill it
func (m *Manager) SetContext(context *helix.Context) {
	m.Lock()
	m.context = context
	m.Unlock()
}

func (m *Manager) HelixDataAccessor() helix.HelixDataAccessor {
	return nil // TODO
}

func (m *Manager) MessagingService() helix.ClusterMessagingService {
	return nil // TODO
}
