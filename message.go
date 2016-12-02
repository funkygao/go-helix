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

func (m Message) MessageType() string {
	return m.GetStringField("MSG_TYPE", "")
}

func (m Message) Resource() string {
	return m.GetStringField("RESOURCE_NAME", "")
}

func (m Message) PartitionName() string {
	return m.GetStringField("PARTITION_NAME", "")
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
