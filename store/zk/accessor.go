package zk

// Helix property accessor.
type dataAccessor struct {
	clusterID string
	conn      *connection
	kb        keyBuilder
}

func newDataAccessor(cluster string, conn *connection) *dataAccessor {
	return &dataAccessor{
		clusterID: cluster,
		conn:      conn,
		kb:        keyBuilder{clusterID: cluster},
	}
}

func (da dataAccessor) create(k, v interface{}) error {
	return nil
}

func (da dataAccessor) set(k, v interface{}) error {
	return nil
}

func (da dataAccessor) get(k interface{}) error {
	return nil
}

func (da dataAccessor) update() {
}

func (da dataAccessor) remove(k interface{}) error {
	return nil
}

func (da dataAccessor) exists(k interface{}) {
}

func (da dataAccessor) watchChildren() {
}

func (da dataAccessor) watchProperty() {
}
