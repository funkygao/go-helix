package model

type LiveInstance struct {
	*Record
}

func NewLiveInstanceFromRecord(r *Record) *LiveInstance {
	return &LiveInstance{Record: r}
}

func (li *LiveInstance) SetSessionID(sessionID string) {
	li.SetStringField("SESSION_ID", sessionID)
}

func (li *LiveInstance) SessionID() string {
	return li.GetStringField("SESSION_ID", "")
}
