package model

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestMessage(t *testing.T) {
	b := []byte(`
	{
     "id": "9ff57fc1-9f2a-41a5-af46-c4ae2a54c539",
     "simpleFields": {
         "CREATE_TIMESTAMP": "1425268051457",
         "ClusterEventName": "currentStateChange",
         "FROM_STATE": "OFFLINE",
         "MSG_ID": "9ff57fc1-9f2a-41a5-af46-c4ae2a54c539",
         "MSG_STATE": "new",
         "MSG_TYPE": "STATE_TRANSITION",
         "PARTITION_NAME": "myDB_5",
         "RESOURCE_NAME": "myDB",
         "SRC_NAME": "precise64-CONTROLLER",
         "SRC_SESSION_ID": "14bd852c528004c",
         "STATE_MODEL_DEF": "MasterSlave",
         "STATE_MODEL_FACTORY_NAME": "DEFAULT",
         "TGT_NAME": "localhost_12913",
         "TGT_SESSION_ID": "93406067297878252",
         "TO_STATE": "SLAVE"
     },
     "listFields": {},
     "mapFields": {}
 }`)

	r, err := NewRecordFromBytes(b)
	assert.Equal(t, nil, err)

	m := NewMessageFromRecord(r)

	// msg id
	assert.Equal(t, "9ff57fc1-9f2a-41a5-af46-c4ae2a54c539", m.ID())

	// validate
	assert.Equal(t, true, m.Valid())

	assert.Equal(t, false, m.IsControllerMsg())

	// message state
	assert.Equal(t, "new", m.MessageState())
	assert.Equal(t, "STATE_TRANSITION", m.MessageType())
	assert.Equal(t, "MasterSlave", m.StateModelDef())
	assert.Equal(t, "currentStateChange", m.ClusterEventName())
	assert.Equal(t, "DEFAULT", m.StateModelFactory())

	assert.Equal(t, "OFFLINE", m.FromState())
	assert.Equal(t, "SLAVE", m.ToState())

	assert.Equal(t, int64(1425268051457), m.CreateTimeStamp())

	assert.Equal(t, "precise64-CONTROLLER", m.SrcName())
	assert.Equal(t, "14bd852c528004c", m.SrcSessionID())
	assert.Equal(t, "myDB", m.Resource())
	assert.Equal(t, "myDB_5", m.PartitionName())

	assert.Equal(t, "localhost_12913", m.TargetName())
	assert.Equal(t, "93406067297878252", m.TargetSessionID())
}
