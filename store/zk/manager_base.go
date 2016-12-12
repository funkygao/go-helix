package zk

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
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

func (m *Manager) IsLeader() bool {
	return false
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

func (m *Manager) MessagingService() helix.ClusterMessagingService {
	return m.messaging
}

func (m *Manager) ClusterManagementTool() helix.HelixAdmin {
	if m.admin == nil {
		m.admin = newZkHelixAdminWithConn(m.conn)
	}
	return m.admin
}

// GetControllerMessages retrieves controller messages from zookeeper
func (s *Manager) GetControllerMessages() ([]*model.Record, error) {
	messages, err := s.conn.Children(s.kb.controllerMessages())
	if err != nil {
		return nil, err
	}

	result := []*model.Record{}
	for _, m := range messages {
		record, err := s.conn.GetRecordFromPath(s.kb.controllerMessage(m))
		if err != nil {
			return result, err
		}

		result = append(result, record)
	}

	return result, nil
}

// GetInstanceMessages retrieves messages sent to an instance
func (s *Manager) GetInstanceMessages(instance string) ([]*model.Record, error) {
	messages, err := s.conn.Children(s.kb.messages(instance))
	if err != nil {
		return nil, err
	}

	result := []*model.Record{}
	for _, m := range messages {
		record, err := s.conn.GetRecordFromPath(s.kb.message(instance, m))
		if err != nil {
			return result, nil
		}

		result = append(result, record)
	}

	return result, nil
}

// GetLiveInstances retrieve a copy of the current live instances.
func (s *Manager) GetLiveInstances() ([]*model.Record, error) {
	instances, err := s.conn.Children(s.kb.liveInstances())
	if err != nil {
		return nil, err
	}

	liveInstances := []*model.Record{}
	for _, participantID := range instances {
		r, err := s.conn.GetRecordFromPath(s.kb.liveInstance(participantID))
		if err != nil {
			return liveInstances, err
		}

		liveInstances = append(liveInstances, r)
	}

	return liveInstances, nil
}

// GetExternalView retrieves a copy of the external views
// TODO return []*mode.ExternalView
func (s *Manager) GetExternalView() []*model.Record {
	result := []*model.Record{}
	for resource, v := range s.externalViewResourceMap {
		if v == false {
			continue
		}

		record, err := s.conn.GetRecordFromPath(s.kb.externalViewForResource(resource))
		if err != nil {
			log.Error("%s %v", s.shortID(), err)
			continue
		}

		result = append(result, record)
	}

	return result
}

// GetIdealState retrieves a copy of the ideal state
func (s *Manager) GetIdealState() []*model.Record {
	result := []*model.Record{}
	for resource, v := range s.idealStateResourceMap {
		if v == false {
			continue
		}

		record, err := s.conn.GetRecordFromPath(s.kb.idealStateForResource(resource))
		if err != nil {
			log.Error("%s %v", s.shortID(), err)
			continue
		}

		result = append(result, record)
	}

	return result
}

// GetCurrentState retrieves a copy of the current state for specified instance
func (s *Manager) GetCurrentState(instance string) []*model.Record {
	resources, err := s.conn.Children(s.kb.instance(instance))
	if err != nil {
		return nil
	}

	result := []*model.Record{}
	for _, r := range resources {
		record, err := s.conn.GetRecordFromPath(s.kb.currentStateForResource(instance, s.conn.GetSessionID(), r))
		if err != nil {
			log.Error("%s %v", s.shortID(), err)
			continue
		}

		result = append(result, record)
	}

	return result
}

// GetInstanceConfigs retrieves a copy of instance configs from zookeeper
func (s *Manager) GetInstanceConfigs() []*model.Record {
	configs, err := s.conn.Children(s.kb.participantConfigs())
	if err != nil {
		return nil
	}

	result := []*model.Record{}
	for _, i := range configs {
		record, err := s.conn.GetRecordFromPath(s.kb.participantConfig(i))
		if err != nil {
			log.Error("%s %v", s.shortID(), err)
			continue
		}

		result = append(result, record)
	}

	return result
}
