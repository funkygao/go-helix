package zk

import (
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
	"github.com/yichen/go-zookeeper/zk"
)

type participant struct {
	*Manager

	stopper chan struct{}
}

func newParticipant(m *Manager) *participant {
	return &participant{
		Manager: m,
		stopper: make(chan struct{}),
	}
}

func (p *participant) createLiveInstance() error {
	record := model.NewLiveInstanceRecord(p.instanceID, p.conn.GetSessionID())
	data := record.Marshal()

	// it is possible the live instance still exists from last run
	// retry 5 seconds to wait for the zookeeper to remove the live instance
	// from previous session
	var (
		backoff = p.conn.sessionTimeout + time.Millisecond*500
		err     error
	)
	for retry := 0; retry < 10; retry++ {
		_, err = p.conn.Create(p.kb.liveInstance(p.instanceID), data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err == nil {
			log.Trace("P[%s/%s] become alive", p.instanceID, p.conn.GetSessionID())
			return nil
		} else if err == zk.ErrNodeExists {
			if c, e := p.conn.Get(p.kb.liveInstance(p.instanceID)); e == zk.ErrNoNode {
				// live instance is gone as we check it, retry create live instance
				continue
			} else {
				if strconv.FormatInt(p.conn.stat.EphemeralOwner, 10) == p.conn.GetSessionID() {
					curLiveInstance, err := model.NewRecordFromBytes(c)
					if err == nil && curLiveInstance.GetStringField("SESSION_ID", "") != p.conn.GetSessionID() {
						// update session id field
						curLiveInstance.SetSimpleField("SESSION_ID", p.conn.GetSessionID())
						p.conn.Set(p.kb.liveInstance(p.instanceID), curLiveInstance.Marshal())
					}
				}
			}
		} else {
			log.Error("P[%s/%s] %v, backoff %s then retry", p.instanceID, p.conn.GetSessionID(), backoff)
		}

		// wait for zookeeper remove the last run's ephemeral znode
		time.Sleep(backoff)
	}

	log.Debug("P[%s/%s] create live instance %v", p.instanceID, p.conn.GetSessionID(), err)

	return err
}

// carry over current-states from last sessions
// set to initial state for current session only when state doesn't exist in current session
func (p *participant) carryOverPreviousCurrentState() error {
	log.Trace("P[%s/%s] cleanup stale sessions", p.instanceID, p.conn.GetSessionID())

	sessions, err := p.conn.Children(p.kb.currentStates(p.instanceID))
	if err != nil {
		return err
	}

	for _, sessionID := range sessions {
		if sessionID == p.conn.GetSessionID() {
			continue
		}

		log.Warn("P[%s/%s] found stale session: %s", p.instanceID, p.conn.GetSessionID(), sessionID)

		if err = p.conn.DeleteTree(p.kb.currentStatesForSession(p.instanceID, sessionID)); err != nil {
			return err
		}
	}

	return nil
}

func (p *participant) autoJoinAllowed() (bool, error) {
	config, err := p.conn.Get(p.kb.clusterConfig())
	if err != nil {
		return false, err
	}

	c, err := model.NewRecordFromBytes(config)
	if err != nil {
		return false, err
	}

	allowed := c.GetSimpleField("allowParticipantAutoJoin")
	if allowed == nil {
		// false by default
		return false, nil
	}

	al, _ := allowed.(string)
	return strings.ToLower(al) == "true", nil
}

// Ensure that ZNodes for a participant all exist.
func (p *participant) joinCluster() (bool, error) {
	exists, err := p.conn.IsInstanceSetup(p.clusterID, p.instanceID)
	if err != nil {
		return false, err
	}

	if exists {
		// this instance is already setup ok
		return true, nil
	}

	allowAutoJoin, err := p.autoJoinAllowed()
	if err != nil {
		return false, err
	}
	if !allowAutoJoin {
		return false, nil
	}

	// the participant path does not exist in zookeeper
	// create the data struture
	participant := model.NewRecord(p.instanceID)
	participant.SetSimpleField("HELIX_HOST", p.host)
	participant.SetSimpleField("HELIX_PORT", p.port)
	participant.SetSimpleField("HELIX_ENABLED", "true")
	// TODO p.ClusterManagementTool().AddNode(p.clusterID, p.instanceID)
	if err = any(
		p.conn.CreateRecordWithPath(p.kb.participantConfig(p.instanceID), participant),

		// /{cluster}/INSTANCES/localhost_12000
		p.conn.CreateEmptyNode(p.kb.instance(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/CURRENTSTATES
		p.conn.CreateEmptyNode(p.kb.currentStates(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/ERRORS
		p.conn.CreateEmptyNode(p.kb.errorsR(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/HEALTHREPORT
		p.conn.CreateEmptyNode(p.kb.healthReport(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/MESSAGES
		p.conn.CreateEmptyNode(p.kb.messages(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/STATUSUPDATES
		p.conn.CreateEmptyNode(p.kb.statusUpdates(p.instanceID)),
	); err != nil {
		return false, err
	}

	return true, nil
}

func (p *participant) setupMsgHandler() {
	// TODO register messaging STATE_TRANSITION

	p.messaging.onConnected()
}
