package helix

// Message is controller generated payload to instruct participant to carry
// out some tasks.
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

func (m Message) StateModel() string {
	return m.GetStringField("STATE_MODEL_DEF", "")
}

func (m Message) TargetName() string {
	return m.GetStringField("TGT_NAME", "")
}

func (m Message) TargetSessionID() string {
	return m.GetStringField("TGT_SESSION_ID", "")
}

func (m *Message) SetExecuteSessionID(sessID string) {
	m.SetSimpleField("EXE_SESSION_ID", sessID)
}

func (m *Message) SetReadTimestamp(val int64) {
	m.SetSimpleField("READ_TIMESTAMP", val)
}
