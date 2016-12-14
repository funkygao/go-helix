package zk

import (
	"sync"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/zkclient"
)

type connection struct {
	*zkclient.Client

	closeOnce sync.Once
}

func newConnection(zkSvr string) *connection {
	conn := &connection{
		Client: zkclient.New(zkSvr),
	}

	return conn
}

func (conn *connection) Disconnect() {
	conn.closeOnce.Do(func() {
		conn.Client.Disconnect()
	})
}

func (conn *connection) GetRecord(path string) (*model.Record, error) {
	data, err := conn.Get(path)
	if err != nil {
		return nil, err
	}
	return model.NewRecordFromBytes(data)
}

func (conn *connection) RemoveMapFieldKey(path string, key string) error {
	data, err := conn.Get(path)
	if err != nil {
		return err
	}

	record, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	record.RemoveMapField(key)
	return conn.Set(path, record.Marshal())
}

// update a map field for the znode. path is the znode path. key is the top-level key in
// the MapFields, mapProperty is the inner key, and value is the. For example:
//
// mapFields":{
// "eat1-app993.stg.linkedin.com_11932,BizProfile,p31_1,SLAVE":{
//   "CURRENT_STATE":"ONLINE"
//   ,"INFO":""
// }
// if we want to set the CURRENT_STATE to ONLINE, we call
// UpdateMapField("/RELAY/INSTANCES/{instance}/CURRENT_STATE/{sessionID}/{db}", "eat1-app993.stg.linkedin.com_11932,BizProfile,p31_1,SLAVE", "CURRENT_STATE", "ONLINE")
func (conn *connection) UpdateMapField(path string, key string, property string, value string) error {
	data, err := conn.Get(path)
	if err != nil {
		return err
	}

	// convert the result into Record
	record, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	record.SetMapField(key, property, value)
	return conn.Set(path, record.Marshal())
}

func (conn *connection) UpdateSimpleField(path string, key string, value string) error {
	data, err := conn.Get(path)
	if err != nil {
		return err
	}

	// convert the result into Record
	record, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	record.SetSimpleField(key, value)
	return conn.Set(path, record.Marshal())
}

func (conn *connection) IsClusterSetup(cluster string) (bool, error) {
	if cluster == "" {
		return false, helix.ErrInvalidArgument
	}
	if !conn.IsConnected() {
		return false, helix.ErrNotConnected
	}

	kb := newKeyBuilder(cluster)
	return conn.ExistsAll(
		kb.cluster(),
		kb.idealStates(),
		kb.participantConfigs(),
		kb.propertyStore(),
		kb.liveInstances(),
		kb.instances(),
		kb.externalView(),
		kb.stateModelDefs(),
		kb.controller(),
		kb.controllerErrors(),
		kb.controllerHistory(),
		kb.controllerMessages(),
		kb.controllerStatusUpdates(),
	)
}

func (conn *connection) IsInstanceSetup(cluster, node string) (bool, error) {
	if cluster == "" || node == "" {
		return false, helix.ErrInvalidArgument
	}
	if !conn.IsConnected() {
		return false, helix.ErrNotConnected
	}

	kb := newKeyBuilder(cluster)
	return conn.ExistsAll(
		kb.participantConfig(node),
		kb.instance(node),
		kb.messages(node),
		kb.currentStates(node),
		kb.errorsR(node),
		kb.statusUpdates(node),
		kb.healthReport(node),
	)
}
