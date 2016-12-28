package zk

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/go-zookeeper/zk"
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
	} else if err == zk.ErrNodeExists {
		log.Warn("%s live instance still exists", p.shortID())
		err = nil
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

	log.Trace("%s joined cluster %s", p.shortID(), p.clusterID)

	return true, nil
}

func (p *participant) setupMsgHandler() {
	log.Debug("%s setup message handler", p.shortID())

	// FIXME each HasSession,
	p.messaging.RegisterMessageHandlerFactory(helix.MessageTypeStateTransition, p.sme)
	p.AddMessageListener(p.instanceID, p.messaging.onMessages)

	// TODO scheduled task

	p.messaging.onConnected()
}
