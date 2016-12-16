package zk

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-zookeeper/zk"
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
	case helix.LiveInstanceChanged:
	case helix.IdealStateChanged:
		l := cb.listener.(helix.IdealStateChangeListener)
		l(nil, nil)

	case helix.CurrentStateChanged:
	case helix.InstanceConfigChanged:
	case helix.ControllerMessagesChanged:
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
