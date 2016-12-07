package model

// Message is controller generated payload to instruct participant to carry out some tasks.
//
// message content example:
// 9ff57fc1-9f2a-41a5-af46-c4ae2a54c539
// {
//     "id": "9ff57fc1-9f2a-41a5-af46-c4ae2a54c539",
//     "simpleFields": {
//         "CREATE_TIMESTAMP": "1425268051457",
//         "ClusterEventName": "currentStateChange",
//         "FROM_STATE": "OFFLINE",
//         "MSG_ID": "9ff57fc1-9f2a-41a5-af46-c4ae2a54c539",
//         "MSG_STATE": "new",
//         "MSG_TYPE": "STATE_TRANSITION",
//         "PARTITION_NAME": "myDB_5",
//         "RESOURCE_NAME": "myDB",
//         "SRC_NAME": "precise64-CONTROLLER",
//         "SRC_SESSION_ID": "14bd852c528004c",
//         "STATE_MODEL_DEF": "MasterSlave",
//         "STATE_MODEL_FACTORY_NAME": "DEFAULT",
//         "TGT_NAME": "localhost_12913",
//         "TGT_SESSION_ID": "93406067297878252",
//         "TO_STATE": "SLAVE"
//     },
//     "listFields": {},
//     "mapFields": {}
// }
//
type Message struct {
	*Record
}

func NewMessageFromRecord(record *Record) *Message {
	return &Message{Record: record}
}

func (m Message) ID() string {
	return m.GetStringField("MSG_ID", "")
}

func (m Message) MessageState() string {
	return m.GetStringField("MSG_STATE", "")
}

func (m *Message) SetMessageState(state string) {
	m.SetSimpleField("MSG_STATE", state)
}

func (m Message) MessageType() string {
	return m.GetStringField("MSG_TYPE", "")
}

func (m Message) Resource() string {
	return m.GetStringField("RESOURCE_NAME", "")
}

func (m Message) PartitionName() string {
	return m.GetStringField("PARTITION_NAME", "")
}

func (m Message) BatchMessageMode() bool {
	return m.GetBooleanField("BATCH_MESSAGE_MODE", false)
}

func (m Message) FromState() string {
	return m.GetStringField("FROM_STATE", "")
}

func (m Message) ToState() string {
	return m.GetStringField("TO_STATE", "")
}

func (m Message) StateModelDef() string {
	return m.GetStringField("STATE_MODEL_DEF", "")
}

// TargetName returns the target instance name.
func (m Message) TargetName() string {
	return m.GetStringField("TGT_NAME", "")
}

func (m Message) TargetSessionID() string {
	return m.GetStringField("TGT_SESSION_ID", "")
}

func (m Message) BucketSize() int {
	return m.GetIntField("BUCKET_SIZE", 0)
}

func (m *Message) SetExecuteSessionID(sessID string) {
	m.SetSimpleField("EXE_SESSION_ID", sessID)
}

func (m *Message) SetReadTimestamp(val int64) {
	m.SetSimpleField("READ_TIMESTAMP", val)
}
