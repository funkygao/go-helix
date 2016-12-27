package zk

import (
	"sync"

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

	msgHandlerFactory map[string]helix.StateMachineEngine

	// a LRU cache of recently received message IDs.
	// Use this to detect new messages and existing messages
	receivedMessages *lru.Cache
}

func newZkMessagingService(m *Manager) *zkMessagingService {
	ms := &zkMessagingService{
		Manager:           m,
		msgHandlerFactory: map[string]helix.StateMachineEngine{},
	}

	var err error
	ms.receivedMessages, err = lru.NewWithEvict(10<<10, nil)
	if err != nil {
		return nil
	}

	return ms
}

func (m *zkMessagingService) RegisterMessageHandlerFactory(messageType string, factory helix.StateMachineEngine) {
	m.lock.Lock()
	m.msgHandlerFactory[messageType] = factory
	m.lock.Unlock()
}

// TODO
func (m *zkMessagingService) Send(msg *model.Message) error {
	return nil
}

func (m *zkMessagingService) HandleChildChange(parentPath string, currentChilds []string) error {
	msgIDs, err := m.conn.Children(parentPath)
	if err != nil {
		return err
	}

	log.Debug("%s msgs: %+v", m.shortID(), msgIDs)

	for _, msgID := range msgIDs {
		if m.receivedMessages.Contains(msgID) {
			continue
		}

		m.receivedMessages.Add(msgID, struct{}{})

		// sequentially processing each message
		if err = m.processMessage(msgID); err != nil {
			log.Error("msg[%s] %v", msgID, err)
		}
	}

	return nil
}

func (p *zkMessagingService) processMessage(msgID string) error {
	record, err := p.conn.GetRecord(p.kb.message(p.instanceID, msgID))
	if err != nil {
		return err
	}

	message := model.NewMessageFromRecord(record)
	factory := p.msgHandlerFactory[message.MessageType()]
	if factory == nil {
		return helix.ErrUnkownMessageType
	}

	if handler := factory.CreateMessageHandler(message, nil); handler != nil {
		return handler.HandleMessage(message)
	}

	return nil
}

func (m *zkMessagingService) onConnected() {

}
