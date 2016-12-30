package zk

import (
	"github.com/funkygao/go-helix"
)

// NewZkParticipant creates a Participant implementation with zk as storage.
func NewZkParticipant(clusterID, host, port, zkSvr string, options ...ManagerOption) (mgr *Manager, err error) {
	return newZkHelixManager(clusterID, host, port, zkSvr, helix.InstanceTypeParticipant, options...)
}

// NewZkSpectator creates a Spectator implementation with zk as storage.
func NewZkSpectator(clusterID, host, port, zkSvr string, options ...ManagerOption) (mgr *Manager, err error) {
	return newZkHelixManager(clusterID, host, port, zkSvr, helix.InstanceTypeSpectator, options...)
}

// NewZkStandaloneController creates a Standalone Controller implementation with zk as storage.
func NewZkStandaloneController(clusterID, host, port, zkSvr string, options ...ManagerOption) (mgr *Manager, err error) {
	return newZkHelixManager(clusterID, host, port, zkSvr, helix.InstanceTypeControllerStandalone, options...)
}

// NewZkDistributedController creates a Distributed Controller implementation with zk as storage.
func NewZkDistributedController(clusterID, host, port, zkSvr string, options ...ManagerOption) (mgr *Manager, err error) {
	return newZkHelixManager(clusterID, host, port, zkSvr, helix.InstanceTypeControllerDistributed, options...)
}

func (m *Manager) Valid() bool {
	// TODO
	return true
}

func (m *Manager) PropertyStore() {}

func (m *Manager) AddPreConnectCallback(cb helix.PreConnectCallback) {
	m.preConnectCallbacks = append(m.preConnectCallbacks, cb)
}

func (m *Manager) AddPostConnectCallback(cb helix.PostConnectCallback) {
	m.postConnectCallbacks = append(m.postConnectCallbacks, cb)
}

func (m *Manager) Cluster() string {
	return m.clusterID
}

func (m *Manager) InstanceType() helix.InstanceType {
	return m.it
}

func (m *Manager) IsLeader() bool {
	if !m.it.IsController() || !m.IsConnected() {
		return false
	}

	return m.ClusterManagementTool().ControllerLeader(m.clusterID) == m.instanceID
}

func (m *Manager) Instance() string {
	return m.instanceID
}

func (m *Manager) SessionID() string {
	return m.conn.SessionID()
}

func (m *Manager) StateMachineEngine() helix.StateMachineEngine {
	return m.sme
}

func (m *Manager) MessagingService() helix.ClusterMessagingService {
	return m.messaging
}

func (m *Manager) ClusterManagementTool() helix.HelixAdmin {
	if m.admin == nil {
		if !m.IsConnected() {
			return nil
		}
		m.admin = newZkHelixAdminWithConn(m.conn)
	}
	return m.admin
}
