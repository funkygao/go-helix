package zk

import (
	"sync"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
	lru "github.com/hashicorp/golang-lru"
)

var (
	_ helix.ClusterMessagingService = &zkMessagingService{}
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

func (m *zkMessagingService) onMessages(instance string, messages []*model.Message, ctx *helix.Context) {
	var msgIDs = make([]string, 0, len(messages))
	for _, msg := range messages {
		msgIDs = append(msgIDs, msg.ID())
	}
	log.Debug("msgs %+v", msgIDs)

	for _, msg := range messages {
		if m.receivedMessages.Contains(msg.ID()) {
			log.Debug("msg[%s] was processed, skipped", msg.ID())
			continue
		}

		m.receivedMessages.Add(msg.ID(), struct{}{})

		if err := m.processMessage(msg); err != nil {
			log.Error("msg[%s] %v", msg.ID(), err)
		}
	}

}

func (p *zkMessagingService) processMessage(message *model.Message) error {
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
