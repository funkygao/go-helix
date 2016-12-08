package zk

import (
	"github.com/funkygao/go-helix"
)

func (m *Manager) ClusterManagementTool() helix.HelixAdmin {
	if m.admin == nil {
		m.admin = newZkHelixAdminWithConn(m.conn)
	}
	return m.admin
}
