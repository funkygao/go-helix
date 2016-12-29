package model

import (
	"strings"
)

// Message is controller generated payload to instruct participant to carry out some tasks.
//
// message content example:
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

// ID returns the unique identifier of this message.
func (m Message) ID() string {
	return m.GetStringField("MSG_ID", "")
}

func (m Message) Valid() bool {
	if strings.ToUpper(m.MessageType()) == "STATE_TRANSITION" {
		notValid := m.TargetName() == "" ||
			m.PartitionName() == "" ||
			m.Resource() == "" ||
			m.StateModelDef() == "" ||
			m.ToState() == "" ||
			m.FromState() == ""
		return !notValid
	}

	return true
}

// IsControllerMsg checks if this message is targetted for a controller.
func (m Message) IsControllerMsg() bool {
	return strings.ToLower(m.TargetName()) == "controller"
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

func (m Message) MessageSubType() string {
	return m.GetStringField("MSG_SUBTYPE", "")
}

// Resource returns the resource associated with this message.
func (m Message) Resource() string {
	return m.GetStringField("RESOURCE_NAME", "")
}

// ResourceGroupName returns the resource group associated with this message.
func (m Message) ResourceGroupName() string {
	return m.GetStringField("RESOURCE_GROUP_NAME", "")
}

// ResourceTag returns the resource tag associated with this message.
func (m Message) ResourceTag() string {
	return m.GetStringField("RESOURCE_TAG", "")
}

// PartitionName returns the name of the partition this message concerns.
func (m Message) PartitionName() string {
	return m.GetStringField("PARTITION_NAME", "")
}

// PartitionNames returns a list of partitions associated with this message.
func (m Message) PartitionNames() []string {
	return m.GetListField("PARTITION_NAME")
}

func (m Message) FromState() string {
	return m.GetStringField("FROM_STATE", "")
}

func (m Message) ToState() string {
	return m.GetStringField("TO_STATE", "")
}

func (m Message) SetToState(state string) {
	m.SetStringField("TO_STATE", state)
}

// StateModelDef returns the state model definition name.
func (m Message) StateModelDef() string {
	return m.GetStringField("STATE_MODEL_DEF", "")
}

// SrcName returns the instance from which the message originated.
func (m Message) SrcName() string {
	return m.GetStringField("SRC_NAME", "")
}

func (m Message) SrcInstanceType() string {
	return m.GetStringField("SRC_INSTANCE_TYPE", "")
}

// TargetName returns the target instance name.
func (m Message) TargetName() string {
	return m.GetStringField("TGT_NAME", "")
}

// SrcSessionID returns the session identifier of the source node.
func (m Message) SrcSessionID() string {
	return m.GetStringField("SRC_SESSION_ID", "")
}

// TargetSessionID returns the session identifier of the destination node.
func (m Message) TargetSessionID() string {
	return m.GetStringField("TGT_SESSION_ID", "")
}

// SetExecuteSessionID sets the session identifier of the node that executes the message.
func (m *Message) SetExecuteSessionID(sessID string) {
	m.SetSimpleField("EXE_SESSION_ID", sessID)
}

// CreateTimeStamp returns the time that this message was created.
func (m Message) CreateTimeStamp() int64 {
	return int64(m.GetIntField("CREATE_TIMESTAMP", 0))
}

func (m Message) RetryCount() int {
	return m.GetIntField("RETRY_COUNT", 0)
}

// ExecutionTimeout returns the time to wait before stopping execution of this message.
func (m Message) ExecutionTimeout() int {
	return m.GetIntField("TIMEOUT", -1)
}

func (m Message) ClusterEventName() string {
	return m.GetStringField("ClusterEventName", "")
}

func (m Message) StateModelFactory() string {
	return m.GetStringField("STATE_MODEL_FACTORY_NAME", "DEFAULT")
}

// SetReadTimestamp sets the time that this message was read.
func (m *Message) SetReadTimestamp(val int64) {
	m.SetSimpleField("READ_TIMESTAMP", val)
}

// SetExecuteStartTimeStamp sets the time that the instance executes tasks as instructed by this message.
func (m *Message) SetExecuteStartTimeStamp(val int64) {
	m.SetSimpleField("EXECUTE_START_TIMESTAMP", val)
}

// TODO
// CreateReplyMessage creates a reply based on an incoming message.
func (m Message) CreateReplyMessage(src *Message) {

}

type Messages []Message

func (ms Messages) Len() int {
	return len(ms)
}

func (ms Messages) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

func (ms Messages) Less(i, j int) bool {
	return ms[i].CreateTimeStamp() < ms[j].CreateTimeStamp()
}
