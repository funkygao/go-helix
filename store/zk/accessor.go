package zk

import (
	"github.com/funkygao/go-helix"
)

var _ helix.HelixDataAccessor = &zkDataAccessor{}

type zkDataAccessor struct {
	clusterID string
	conn      *connection
	kb        keyBuilder
}

func newZkDataAccessor(cluster string, conn *connection) *zkDataAccessor {
	return &zkDataAccessor{
		clusterID: cluster,
		conn:      conn,
		kb:        keyBuilder{clusterID: cluster},
	}
}

func (da zkDataAccessor) create(k, v interface{}) error {
	return nil
}

func (da zkDataAccessor) set(k, v interface{}) error {
	return nil
}

func (da zkDataAccessor) get(k interface{}) error {
	return nil
}

func (da zkDataAccessor) update() {
}

func (da zkDataAccessor) remove(k interface{}) error {
	return nil
}

func (da zkDataAccessor) exists(k interface{}) {
}

func (da zkDataAccessor) watchChildren() {
}

func (da zkDataAccessor) watchProperty() {
}
