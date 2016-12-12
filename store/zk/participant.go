package zk

import (
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/go-zookeeper/zk"
	log "github.com/funkygao/log4go"
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
	log.Debug("%s creating live instance...", p.shortID())

	record := model.NewLiveInstanceRecord(p.instanceID, p.conn.SessionID())
	data := record.Marshal()

	// it is possible the live instance still exists from last run
	// retry 5 seconds to wait for the zookeeper to remove the live instance
	// from previous session
	var (
		backoff = p.conn.sessionTimeout + time.Millisecond*50
		err     error
	)
	for retry := 0; retry < 10; retry++ {
		err = p.conn.CreateEphemeral(p.kb.liveInstance(p.instanceID), data)
		if err == nil {
			break
		} else if err == zk.ErrNodeExists {
			if c, e := p.conn.Get(p.kb.liveInstance(p.instanceID)); e == zk.ErrNoNode {
				log.Trace("%s live instance is gone as we check it, retry create live instance", p.shortID())
				continue // needn't sleep backoff
			} else {
				currentSessionID := strconv.FormatInt(p.conn.stat.EphemeralOwner, 10)
				log.Debug("%s current session: %s, same: %+v", p.shortID(), currentSessionID, currentSessionID == p.conn.SessionID())
				if currentSessionID == p.conn.SessionID() {
					curLiveInstance, err := model.NewRecordFromBytes(c)
					if err == nil && curLiveInstance.GetStringField("SESSION_ID", "") != p.conn.SessionID() {
						log.Trace("%s update session id field", p.shortID())

						curLiveInstance.SetSimpleField("SESSION_ID", p.conn.SessionID())
						p.conn.Set(p.kb.liveInstance(p.instanceID), curLiveInstance.Marshal())
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

	return err
}

// carry over current-states from last sessions
// set to initial state for current session only when state doesn't exist in current session
func (p *participant) carryOverPreviousCurrentState() error {
	log.Trace("%s cleanup stale sessions", p.shortID())

	sessions, err := p.conn.Children(p.kb.currentStates(p.instanceID))
	if err != nil {
		return err
	}

	for _, sessionID := range sessions {
		if sessionID == p.conn.SessionID() {
			continue
		}

		log.Warn("%s found stale session: %s, will be removed", p.shortID(), sessionID)

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
	log.Debug("%s join cluster...", p.shortID())

	exists, err := p.conn.IsInstanceSetup(p.clusterID, p.instanceID)
	if err != nil {
		return false, err
	}
	if exists {
		// this instance is already setup ok
		log.Debug("%s instance setup ok", p.shortID())
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
	log.Debug("%s auto setup instance", p.shortID())
	participant := model.NewRecord(p.instanceID)
	participant.SetSimpleField("HELIX_HOST", p.host)
	participant.SetSimpleField("HELIX_PORT", p.port)
	participant.SetSimpleField("HELIX_ENABLED", "true")
	// TODO p.ClusterManagementTool().AddNode(p.clusterID, p.instanceID)
	if err = any(
		p.conn.CreatePersistentRecord(p.kb.participantConfig(p.instanceID), participant),

		// /{cluster}/INSTANCES/localhost_12000
		p.conn.CreateEmptyPersistent(p.kb.instance(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/CURRENTSTATES
		p.conn.CreateEmptyPersistent(p.kb.currentStates(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/ERRORS
		p.conn.CreateEmptyPersistent(p.kb.errorsR(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/HEALTHREPORT
		p.conn.CreateEmptyPersistent(p.kb.healthReport(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/MESSAGES
		p.conn.CreateEmptyPersistent(p.kb.messages(p.instanceID)),

		// /{cluster}/INSTANCES/localhost_12000/STATUSUPDATES
		p.conn.CreateEmptyPersistent(p.kb.statusUpdates(p.instanceID)),
	); err != nil {
		return false, err
	}

	return true, nil
}

func (p *participant) setupMsgHandler() {
	// TODO register messaging STATE_TRANSITION

	p.messaging.onConnected()
}
