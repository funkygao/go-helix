package zk

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
)

type participant struct {
	*Manager

	liveRetry int
}

func newParticipant(m *Manager) *participant {
	return &participant{
		Manager:   m,
		liveRetry: 10,
	}
}

func (p *participant) createLiveInstance() (err error) {
	log.Trace("%s creating live instance...", p.shortID())

	record := model.NewLiveInstanceRecord(p.instanceID, p.conn.SessionID())
	if err = p.conn.CreateLiveNode(p.kb.liveInstance(p.instanceID), record.Marshal(), p.liveRetry); err == nil {
		log.Trace("%s created live instance", p.shortID())
	}

	return
}

func (p *participant) carryOverPreviousCurrentState() error {
	log.Trace("%s start scavenger for stale sessions", p.shortID())

	sessions, err := p.conn.Children(p.kb.currentStates(p.instanceID))
	if err != nil {
		return err
	}

	for _, sessionID := range sessions {
		if sessionID == p.conn.SessionID() {
			continue
		}

		log.Trace("%s removing stale session: %s", p.shortID(), sessionID)
		if err = p.conn.DeleteTree(p.kb.currentStatesForSession(p.instanceID, sessionID)); err != nil {
			return err
		}
	}

	return nil
}

func (p *participant) autoJoinAllowed() (bool, error) {
	r, err := p.conn.GetRecord(p.kb.clusterConfig())
	if err != nil {
		return false, err
	}

	return r.GetBooleanField("allowParticipantAutoJoin", false), nil
}

func (p *participant) joinCluster() (bool, error) {
	log.Debug("%s join cluster...", p.shortID())

	exists, err := p.conn.IsInstanceSetup(p.clusterID, p.instanceID)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	if allowAutoJoin, err := p.autoJoinAllowed(); !allowAutoJoin || err != nil {
		return false, err
	}

	if err = p.ClusterManagementTool().AddNode(p.clusterID, p.instanceID); err != nil {
		return false, err
	}

	return true, nil
}

func (p *participant) setupMsgHandler() {
	p.messaging.enableMessage(helix.MessageTypeStateTransition,
		helix.MessageTypeNoOp)
	p.messaging.onConnected()
}
