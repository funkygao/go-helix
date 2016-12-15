package zk

import (
	"strconv"
	"time"

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
	data := record.Marshal()

	// it is possible the live instance still exists from last run
	// retry to wait for the zookeeper to remove the live instance
	// from previous session
	backoff := p.conn.SessionTimeout() + time.Millisecond*50
	for retry := 0; retry < p.liveRetry; retry++ {
		err = p.conn.CreateEphemeral(p.kb.liveInstance(p.instanceID), data)
		if err == nil {
			break
		} else if err == zk.ErrNodeExists {
			if cur, e := p.conn.Get(p.kb.liveInstance(p.instanceID)); e == zk.ErrNoNode {
				log.Trace("%s live instance is gone as we check it, retry creating live instance", p.shortID())
				continue // needn't sleep backoff
			} else {
				// FIXME LastStat race condition, p.conn.SessionID might be changing
				currentSessionID := strconv.FormatInt(p.conn.LastStat().EphemeralOwner, 10)
				if currentSessionID == p.conn.SessionID() {
					if r, e1 := model.NewRecordFromBytes(cur); e1 == nil {
						curLiveInstance := model.NewLiveInstanceFromRecord(r)
						if curLiveInstance.SessionID() != p.conn.SessionID() {
							log.Trace("%s update session id field", p.shortID())

							curLiveInstance.SetSessionID(p.conn.SessionID())
							p.conn.SetRecord(p.kb.liveInstance(p.instanceID), curLiveInstance)
						}
					}
				} else {
					log.Warn("%s await previous session expire...", p.shortID())
				}
			}
		}

		// wait for zookeeper remove the last run's ephemeral znode
		log.Debug("%s retry=%d backoff: %s", p.shortID(), retry, backoff)
		time.Sleep(backoff)
	}

	if err == nil {
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
	// TODO register messaging STATE_TRANSITION

	p.messaging.onConnected()
}
