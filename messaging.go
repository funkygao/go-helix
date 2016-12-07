package helix

import (
	"github.com/funkygao/go-helix/model"
)

// ClusterMessagingService can be used to send cluster wide messages.
//
// Send message to a specific component in the cluster[participant, controller]
// Broadcast message to all participants
// Send message to instances that hold a specific resource
type ClusterMessagingService interface {
	send(*model.Message) error
}
