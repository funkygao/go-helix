package zk

import (
	"github.com/funkygao/go-helix"
)

func (m *Manager) ClusterManagementTool() helix.HelixAdmin {
	if m.admin == nil {
		// TODO
	}
	return m.admin
}
