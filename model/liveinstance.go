package model

import (
	"fmt"
	"os"

	"github.com/funkygao/go-helix/ver"
)

type LiveInstance struct {
	*Record
}

// NewLiveInstanceRecord creates a new instance of Record for representing a live instance.
func NewLiveInstanceRecord(participantID string, sessionID string) *Record {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	node := NewRecord(participantID)
	node.SetSimpleField("HELIX_VERSION", ver.Ver)
	node.SetSimpleField("SESSION_ID", sessionID)
	node.SetSimpleField("LIVE_INSTANCE", fmt.Sprintf("%d@%s", os.Getpid(), hostname))

	return node
}

func NewLiveInstanceFromRecord(record *Record) *LiveInstance {
	return &LiveInstance{Record: record}
}

func (li *LiveInstance) SetSessionID(sessionID string) {
	li.SetStringField("SESSION_ID", sessionID)
}

func (li *LiveInstance) SessionID() string {
	return li.GetStringField("SESSION_ID", "")
}

func (li *LiveInstance) Node() string {
	return li.ID
}
