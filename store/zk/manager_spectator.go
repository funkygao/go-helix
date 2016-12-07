package zk

import (
	//"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
)

// GetControllerMessages retrieves controller messages from zookeeper
func (s *Manager) GetControllerMessages() []*model.Record {
	result := []*model.Record{}

	messages, err := s.conn.Children(s.kb.controllerMessages())
	if err != nil {
		return result
	}

	for _, m := range messages {
		record, err := s.conn.GetRecordFromPath(s.kb.controllerMessage(m))
		if err == nil {
			result = append(result, record)
		} else {
			// TODO handle the err
		}
	}

	return result
}

// GetInstanceMessages retrieves messages sent to an instance
func (s *Manager) GetInstanceMessages(instance string) []*model.Record {
	result := []*model.Record{}

	messages, err := s.conn.Children(s.kb.messages(instance))
	if err != nil {
		return result
	}

	for _, m := range messages {
		record, err := s.conn.GetRecordFromPath(s.kb.message(instance, m))
		if err == nil {
			result = append(result, record)
		} else {
			// TODO
		}
	}

	return result
}

// GetLiveInstances retrieve a copy of the current live instances.
func (s *Manager) GetLiveInstances() ([]*model.Record, error) {
	liveInstances := []*model.Record{}

	instances, err := s.conn.Children(s.kb.liveInstances())
	if err != nil {
		return nil, err
	}

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
func (s *Manager) GetExternalView() []*model.Record {
	result := []*model.Record{}

	for k, v := range s.externalViewResourceMap {
		if v == false {
			continue
		}

		record, err := s.conn.GetRecordFromPath(s.kb.externalViewForResource(k))

		if err == nil {
			result = append(result, record)
			continue
		}
	}

	return result
}

// GetIdealState retrieves a copy of the ideal state
func (s *Manager) GetIdealState() []*model.Record {
	result := []*model.Record{}

	for k, v := range s.idealStateResourceMap {
		if v == false {
			continue
		}

		record, err := s.conn.GetRecordFromPath(s.kb.idealStateForResource(k))

		if err == nil {
			result = append(result, record)
			continue
		}
	}
	return result
}

// GetCurrentState retrieves a copy of the current state for specified instance
func (s *Manager) GetCurrentState(instance string) []*model.Record {
	result := []*model.Record{}

	resources, err := s.conn.Children(s.kb.instance(instance))
	must(err)

	for _, r := range resources {
		record, err := s.conn.GetRecordFromPath(s.kb.currentStateForResource(instance, s.conn.GetSessionID(), r))
		if err == nil {
			result = append(result, record)
		}
	}

	return result
}

// GetInstanceConfigs retrieves a copy of instance configs from zookeeper
func (s *Manager) GetInstanceConfigs() []*model.Record {
	result := []*model.Record{}

	configs, err := s.conn.Children(s.kb.participantConfigs())
	must(err)

	for _, i := range configs {
		record, err := s.conn.GetRecordFromPath(s.kb.participantConfig(i))
		if err == nil {
			result = append(result, record)
		}
	}

	return result
}
