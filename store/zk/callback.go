package zk

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-zookeeper/zk"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/zkclient"
)

var _ zkclient.ZkChildListener = &CallbackHandler{}
var _ zkclient.ZkDataListener = &CallbackHandler{}

type CallbackHandler struct {
	*Manager

	path       string
	changeType helix.ChangeNotificationType
	listener   interface{}
}

func newCallbackHandler(mgr *Manager, path string, listener interface{},
	changeType helix.ChangeNotificationType, events []zk.EventType) *CallbackHandler {
	return &CallbackHandler{
		Manager:    mgr,
		listener:   listener,
		path:       path,
		changeType: changeType,
	}
}

func (cb *CallbackHandler) Init() {

}

func (cb *CallbackHandler) Reset() {

}

func (cb *CallbackHandler) invoke(cn helix.ChangeNotification) {
	switch cn.ChangeType {
	case helix.ExternalViewChanged:
		if l, ok := cb.listener.(helix.ExternalViewChangeListener); ok {
			l(nil, nil)
		} else {
			log.Error("Initialization with wrong listener type")
		}

	case helix.LiveInstanceChanged:
		if l, ok := cb.listener.(helix.LiveInstanceChangeListener); ok {
			l(nil, nil)
		}

	case helix.IdealStateChanged:
		if l, ok := cb.listener.(helix.IdealStateChangeListener); ok {
			l(nil, nil)
		}

	case helix.CurrentStateChanged:
		if l, ok := cb.listener.(helix.CurrentStateChangeListener); ok {
			l("", nil, nil)
		}

	case helix.InstanceConfigChanged:
		if l, ok := cb.listener.(helix.InstanceConfigChangeListener); ok {
			l(nil, nil)
		}

	case helix.ControllerChanged:
		if l, ok := cb.listener.(helix.ControllerChangeListener); ok {
			l(nil)
		}

	case helix.ControllerMessagesChanged:
		if l, ok := cb.listener.(helix.ControllerMessageListener); ok {
			l(nil, nil)
		}

	case helix.InstanceMessagesChanged:
	}
}

func (cb *CallbackHandler) HandleChildChange(parentPath string, currentChilds []string) error {
	return nil
}

func (cb *CallbackHandler) HandleDataChange(dataPath string, data []byte) error {
	return nil
}

func (cb *CallbackHandler) HandleDataDeleted(dataPath string) error {
	return nil
}
