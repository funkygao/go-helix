package zk

import (
	"time"

	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/go-helix/ver"
	log "github.com/funkygao/log4go"
)

type spectator struct {
	*Manager
}

func (s *spectator) createLiveNode() (err error) {
	t1 := time.Now()
	record := model.NewRecord(s.instanceID)
	record.SetStringField("HELIX_VERSION", ver.Ver)
	err = s.conn.CreateLiveNode(s.kb.liveSpectator(s.instanceID), record.Marshal(), 3)
	log.Trace("handle new session as spectator in %s", time.Since(t1))
	return
}
