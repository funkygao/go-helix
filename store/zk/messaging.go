package zk

import (
	"strings"
	"sync"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/zkclient"
	lru "github.com/hashicorp/golang-lru"
)

var (
	_ helix.ClusterMessagingService = &zkMessagingService{}
	_ zkclient.ZkChildListener      = &zkMessagingService{}
)

// TODO support other types of message besides STATE_TRANSITION
type zkMessagingService struct {
	*Manager

	lock sync.RWMutex

	messageTypes map[string]struct{}

	// a LRU cache of recently received message IDs.
	// Use this to detect new messages and existing messages
	receivedMessages *lru.Cache
}

func newZkMessagingService(m *Manager) *zkMessagingService {
	ms := &zkMessagingService{
		Manager:      m,
		messageTypes: map[string]struct{}{},
	}

	var err error
	ms.receivedMessages, err = lru.New(10 << 10)
	if err != nil {
		return nil
	}

	return ms
}

func (m *zkMessagingService) Send(msg *model.Message) error {
	return nil
}

func (m *zkMessagingService) enableMessage(messageTypes ...string) {
	m.lock.Lock()
	for _, t := range messageTypes {
		m.messageTypes[t] = struct{}{}
	}
	m.lock.Unlock()
}

func (m *zkMessagingService) HandleChildChange(parentPath string, currentChilds []string) error {
	msgIDs, err := m.conn.Children(parentPath)
	if err != nil {
		return err
	}

	log.Debug("%s handle child change: %+v", m.shortID(), msgIDs)

	for _, msgID := range msgIDs {
		// TODO handle outstanding duplicated messages
		if err = m.processMessage(msgID); err != nil {
			log.Error("%v", err)
		}
	}

	return nil
}

func (m *zkMessagingService) onConnected() {
	path := m.kb.messages(m.instanceID)
	log.Trace("%s watching %s", m.shortID(), path)

	m.conn.SubscribeChildChanges(path, m)
}

func (p *zkMessagingService) processMessage(msgID string) error {
	log.Debug("%s processing msg: %s", p.shortID(), msgID)

	msgPath := p.kb.message(p.instanceID, msgID)
	record, err := p.conn.GetRecord(msgPath)
	if err != nil {
		return err
	}

	message := model.NewMessageFromRecord(record)
	msgType := message.MessageType()
	if _, present := p.messageTypes[message.MessageType()]; !present {
		return helix.ErrUnkownMessageType
	}

	if msgType == helix.MessageTypeNoOp {
		log.Warn("%s discard NO-OP message: %s", p.shortID(), msgID)
		return p.conn.DeleteTree(msgPath)
	}

	targetSessionID := message.TargetSessionID()
	if targetSessionID != "*" && targetSessionID != p.conn.SessionID() {
		// message comes from expired session
		log.Warn("%s got mismatched message: %s/%s, dropped", p.shortID(), msgID, targetSessionID)
		return p.conn.DeleteTree(msgPath)
	}

	if !strings.EqualFold(message.MessageState(), helix.MessageStateNew) {
		// READ message is not deleted until the state has changed
		log.Warn("%s skip %s message: %s", p.shortID(), message.MessageState(), msgID)
		return nil
	}

	// update msgState to read
	message.SetMessageState(helix.MessageStateRead)
	message.SetReadTimestamp(time.Now().Unix())
	message.SetExecuteSessionID(p.conn.SessionID())

	// create current state meta data
	// do it for non-controller and state transition messages only
	targetName := message.TargetName()
	if !strings.EqualFold(targetName, string(helix.InstanceTypeControllerStandalone)) &&
		strings.EqualFold(msgType, helix.MessageTypeStateTransition) {
		resourceID := message.Resource()

		currentStateRecord := model.NewRecord(resourceID)
		currentStateRecord.SetBucketSize(message.BucketSize())
		currentStateRecord.SetBatchMessageMode(message.BatchMessageMode())
		currentStateRecord.SetSimpleField("STATE_MODEL_DEF", message.GetSimpleField("STATE_MODEL_DEF"))
		currentStateRecord.SetSimpleField("SESSION_ID", targetSessionID)
		currentStateRecord.SetSimpleField("STATE_MODEL_FACTORY_NAME", message.GetStringField("STATE_MODEL_FACTORY_NAME", "DEFAULT"))

		resourceCurrentStatePath := p.kb.currentStateForResource(p.instanceID, targetSessionID, resourceID)
		exists, err := p.conn.Exists(resourceCurrentStatePath)
		if err != nil {
			return err
		}

		if !exists {
			// only set the current state to zookeeper when it is not present
			if err = p.conn.SetRecord(resourceCurrentStatePath, currentStateRecord); err != nil {
				return err
			}
		}
	}

	if message.MessageType() == helix.MessageTypeStateTransition {
		handler := newTransitionMessageHandler(p.Manager, message)
		handler.handleMessage()
	}

	// after the message is processed successfully, remove it
	return p.conn.DeleteTree(msgPath)
}
