package zk

import (
	"github.com/funkygao/go-helix/model"
)

type transitionMessageHandler struct {
	message *model.Message
}

func newTransitionMessageHandler(message *model.Message) *transitionMessageHandler {
	return &transitionMessageHandler{
		message: message,
	}
}

func (h *transitionMessageHandler) preHandleMessage() {

}

func (h *transitionMessageHandler) postHandleMessage() {

}

func (h *transitionMessageHandler) invoke() {

}

func (h *transitionMessageHandler) handleMessage() {
	h.preHandleMessage()
	h.invoke()
	h.postHandleMessage()
}
