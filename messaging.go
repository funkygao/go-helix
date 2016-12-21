package helix

import (
	"github.com/funkygao/go-helix/model"
)

// ClusterMessagingService can be used to send cluster wide messages.
//
// Send message to a specific component in the cluster[participant, controller].
// Broadcast message to all participants.
// Send message to instances that hold a specific resource.
type ClusterMessagingService interface {
	Send(*model.Message) error

	// RegisterMessageHandler will register a message handler for given type of message.
	RegisterMessageHandler(messageType string)
}
