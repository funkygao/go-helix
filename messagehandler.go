package helix

import (
	"github.com/funkygao/go-helix/model"
)

type MessageHandler interface {
	HandleMessage(*model.Message) error
}
