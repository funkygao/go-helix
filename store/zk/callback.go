package zk

import (
	"fmt"
	gopath "path"

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
	return fmt.Sprintf("%s^%s", cb.path, helix.ChangeNotificationText(cb.changeType))
}

func (cb *CallbackHandler) Init() {
	cb.subscribeForChanges(cb.path)
}

func (cb *CallbackHandler) Reset() {
	log.Trace("%s reset callback for %s", cb.shortID(), cb)

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
			childPath := gopath.Join(path, child)
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
			cb.conn.SubscribeChildChanges(gopath.Join(path, record.ID), cb)
			cb.conn.SubscribeDataChanges(gopath.Join(path, record.ID), cb)
		}

	default:
		for _, child := range children {
			cb.conn.SubscribeDataChanges(gopath.Join(path, child), cb)
		}

	}
}

func (cb *CallbackHandler) invokeListener(cn helix.ChangeNotification) {
	log.Debug("%s invoking listener %+v", cb.shortID(), cn)

	switch cn.ChangeType {
	case helix.ExternalViewChanged:
		if l, ok := cb.listener.(helix.ExternalViewChangeListener); ok {
			data := cn.ChangeData.([]*model.ExternalView)
			l(data, nil)
		}

	case helix.LiveInstanceChanged:
		if l, ok := cb.listener.(helix.LiveInstanceChangeListener); ok {
			data := cn.ChangeData.([]*model.LiveInstance)
			l(data, nil)
		}

	case helix.IdealStateChanged:
		if l, ok := cb.listener.(helix.IdealStateChangeListener); ok {
			data := cn.ChangeData.([]*model.IdealState)
			l(data, nil)
		}

	case helix.CurrentStateChanged:
		if l, ok := cb.listener.(helix.CurrentStateChangeListener); ok {
			data := cn.ChangeData.([]*model.CurrentState)
			l("", data, nil)
		}

	case helix.InstanceConfigChanged:
		if l, ok := cb.listener.(helix.InstanceConfigChangeListener); ok {
			data := cn.ChangeData.([]*model.InstanceConfig)
			l(data, nil)
		}

	case helix.ControllerChanged:
		if l, ok := cb.listener.(helix.ControllerChangeListener); ok {
			// TODO
			l(nil)
		}

	case helix.ControllerMessagesChanged:
		if l, ok := cb.listener.(helix.ControllerMessageListener); ok {
			// TODO
			l(nil, nil)
		}

	case helix.InstanceMessagesChanged:
		// TODO
	}
}

func (cb *CallbackHandler) HandleChildChange(parentPath string, currentChilds []string) error {
	switch cb.changeType {
	case helix.ExternalViewChanged:
		resources, err := cb.conn.Children(parentPath)
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
		liveInstances, values, err := cb.conn.ChildrenValues(cb.kb.liveInstances())
		if err != nil {
			return err
		}

		datas := make([]*model.LiveInstance, 0, len(liveInstances))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				return err
			}
			datas = append(datas, model.NewLiveInstanceFromRecord(record))
		}

		notify := helix.ChangeNotification{
			ChangeType: helix.LiveInstanceChanged,
			ChangeData: datas,
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
