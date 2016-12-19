package zk

import (
	"fmt"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/go-zookeeper/zk"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/zkclient"
)

var (
	_ zkclient.ZkChildListener = &CallbackHandler{}
	_ zkclient.ZkDataListener  = &CallbackHandler{}
)

type CallbackHandler struct {
	*Manager

	path       string
	changeType helix.ChangeNotificationType
	listener   interface{}

	externalViewResourceMap map[string]bool // key is resource
	idealStateResourceMap   map[string]bool // key is resource
	instanceConfigMap       map[string]bool // key is resource
}

func newCallbackHandler(mgr *Manager, path string, listener interface{},
	changeType helix.ChangeNotificationType, events []zk.EventType) *CallbackHandler {
	return &CallbackHandler{
		Manager:                 mgr,
		listener:                listener,
		path:                    path,
		changeType:              changeType,
		externalViewResourceMap: map[string]bool{},
		idealStateResourceMap:   map[string]bool{},
		instanceConfigMap:       map[string]bool{},
	}
}

func (cb *CallbackHandler) String() string {
	return fmt.Sprintf("%s %s", cb.path, helix.ChangeNotificationText(cb.changeType))
}

func (cb *CallbackHandler) Init() {
	switch cb.changeType {
	case helix.ExternalViewChanged:
		cb.Manager.conn.SubscribeChildChanges(cb.kb.externalView(), cb)

	case helix.LiveInstanceChanged:
		cb.Manager.conn.SubscribeDataChanges(cb.kb.instance(cb.instanceID), cb)

	case helix.IdealStateChanged:
		cb.Manager.conn.SubscribeChildChanges(cb.kb.idealStates(), cb)

	case helix.CurrentStateChanged:

	case helix.InstanceConfigChanged:

	case helix.ControllerChanged:

	case helix.ControllerMessagesChanged:
	}

}

func (cb *CallbackHandler) subscribeForChanges(path string) {
	cb.Manager.conn.SubscribeChildChanges(path, cb)
	children, err := cb.conn.Children(path)
	if err != nil {
		log.Error("%v", err)
		return
	}

	switch cb.changeType {
	case helix.ExternalViewChanged, helix.IdealStateChanged, helix.CurrentStateChanged:
		for _, child := range children {
			childPath := fmt.Sprintf("%s/%s", path, child)
			data, err := cb.conn.Get(childPath)
			if err != nil {
				log.Error("%v", err)
				continue
			}

			record, err := model.NewRecordFromBytes(data)
			if err != nil {
				log.Error("%v", err)
				continue
			}
			cb.conn.SubscribeChildChanges(fmt.Sprintf("%s/%s", path, record.ID), cb)
			cb.conn.SubscribeDataChanges(fmt.Sprintf("%s/%s", path, record.ID), cb)
		}

	default:
		for _, child := range children {
			cb.conn.SubscribeDataChanges(fmt.Sprintf("%s/%s", path, child), cb)
		}

	}
}

func (cb *CallbackHandler) Reset() {
	log.Debug("%s reset callback for %s", cb.shortID(), cb.changeType)

	switch cb.changeType {
	case helix.ExternalViewChanged:

	case helix.LiveInstanceChanged:

	case helix.IdealStateChanged:

	case helix.CurrentStateChanged:

	case helix.InstanceConfigChanged:

	case helix.ControllerChanged:

	case helix.ControllerMessagesChanged:
	}
}

func (cb *CallbackHandler) invokeListener(cn helix.ChangeNotification) {
	log.Debug("%s invoke %+v", cb.shortID(), cn)

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
	switch cb.changeType {
	case helix.ExternalViewChanged:
		resources, err := cb.conn.Children(parentPath)
		log.Debug("%s %+v %v", cb.shortID(), resources, err)
		if err != nil {
			return err
		}

		for _, resource := range resources {
			if _, present := cb.externalViewResourceMap[resource]; !present {
				cb.conn.SubscribeDataChanges(cb.kb.externalViewForResource(resource), cb)
				cb.externalViewResourceMap[resource] = true
			}
		}

	case helix.LiveInstanceChanged:
		liveInstances, err := cb.conn.Children(cb.kb.liveInstances())
		log.Debug("%s %+v %v", cb.shortID(), liveInstances, err)
		if err != nil {
			return err
		}

		notify := helix.ChangeNotification{
			ChangeType: helix.LiveInstanceChanged,
			ChangeData: liveInstances,
		}
		cb.invokeListener(notify)

	case helix.IdealStateChanged:
		cb.Manager.conn.SubscribeChildChanges(cb.kb.idealStates(), cb)

	case helix.CurrentStateChanged:

	case helix.InstanceConfigChanged:

	case helix.ControllerChanged:

	case helix.ControllerMessagesChanged:

	default:
		return helix.ErrInvalidArgument
	}

	return nil
}

func (cb *CallbackHandler) HandleDataChange(dataPath string, data []byte) error {
	return nil
}

func (cb *CallbackHandler) HandleDataDeleted(dataPath string) error {
	return nil
}
