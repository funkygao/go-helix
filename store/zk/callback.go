package zk

import (
	"fmt"
	gopath "path"
	"strings"

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

func (cb *CallbackHandler) String() string {
	return fmt.Sprintf("%s^%s", cb.path, helix.ChangeNotificationText(cb.changeType))
}

func (cb *CallbackHandler) Init() {
	log.Trace("%s %s init callback", cb.shortID(), cb)

	cb.invoke(helix.ChangeNotification{
		ChangeType: helix.CallbackInit,
	})

	//cb.subscribeForChanges(cb.path)
}

func (cb *CallbackHandler) Reset() {
	log.Trace("%s %s reset callback", cb.shortID(), cb)

	cb.invoke(helix.ChangeNotification{
		ChangeType: helix.CallbackFinalize,
	})
}

func (cb *CallbackHandler) subscribeDataChanges(path string, ctx helix.ChangeNotification) {
	switch ctx.ChangeType {
	case helix.CallbackInit:
		log.Debug("%s subscribe data changes: %s %s", cb.shortID(), path, ctx)
		cb.conn.SubscribeDataChanges(path, cb)

	case helix.CallbackInvoke:

	case helix.CallbackFinalize:
		log.Debug("%s unsubscribe data changes: %s %s", cb.shortID(), path, ctx)
		cb.conn.UnsubscribeDataChanges(path, cb)
	}
}

func (cb *CallbackHandler) subscribeChildChanges(path string, ctx helix.ChangeNotification) {
	switch ctx.ChangeType {
	case helix.CallbackInit:
		log.Debug("%s subscribe child changes: %s %s", cb.shortID(), path, ctx)
		cb.conn.SubscribeChildChanges(path, cb)

	case helix.CallbackInvoke:

	case helix.CallbackFinalize:
		log.Debug("%s unsubscribe child changes: %s %s", cb.shortID(), path, ctx)
		cb.conn.UnsubscribeChildChanges(path, cb)
	}
}

func (cb *CallbackHandler) subscribeForChanges(path string, ctx helix.ChangeNotification) {
	cb.subscribeChildChanges(path, ctx)

	switch cb.changeType {
	case helix.ExternalViewChanged, helix.IdealStateChanged, helix.CurrentStateChanged:
		children, err := cb.conn.Children(path)
		if err != nil {
			log.Error("%v %s", err, ctx)
			return
		}

		for _, child := range children {
			data, err := cb.conn.Get(gopath.Join(path, child))
			if err != nil {
				log.Error("%v", err)
				continue
			}

			record, err := model.NewRecordFromBytes(data)
			if err != nil {
				log.Error("%v", err)
				continue
			}

			cb.subscribeChildChanges(gopath.Join(path, record.ID), ctx)
			cb.subscribeDataChanges(gopath.Join(path, record.ID), ctx)
		}

	case helix.LiveInstanceChanged:
		children, err := cb.conn.Children(path)
		if err != nil {
			log.Error("%v", err)
			return
		}

		for _, child := range children {
			cb.subscribeDataChanges(gopath.Join(path, child), ctx)
		}

	default:
		log.Warn("unkown type: %s", ctx)

	}
}

func (cb *CallbackHandler) invoke(ctx helix.ChangeNotification) {
	log.Debug("%s invoking listener %s", cb.shortID(), ctx)

	// TODO
	//cb.Manager.Lock()
	//defer cb.Manager.Unlock()

	cb.subscribeForChanges(cb.path, ctx)

	switch cb.changeType {
	case helix.ExternalViewChanged:
		// TODO what if resource deleted
		resources, values, err := cb.conn.ChildrenValues(cb.kb.externalView())
		if err != nil {
			log.Error(err)
			return
		}

		datas := make([]*model.ExternalView, 0, len(resources))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				log.Error(err)
				return
			}

			datas = append(datas, model.NewExternalViewFromRecord(record))
		}
		if l, ok := cb.listener.(helix.ExternalViewChangeListener); ok {
			l(datas, nil)
		} else {
			// TODO
		}

	case helix.IdealStateChanged:
		resources, values, err := cb.conn.ChildrenValues(cb.kb.idealStates())
		if err != nil {
			log.Error(err)
			return
		}

		datas := make([]*model.IdealState, 0, len(resources))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				log.Error(err)
				return
			}

			datas = append(datas, model.NewIdealStateFromRecord(record))
		}
		if l, ok := cb.listener.(helix.IdealStateChangeListener); ok {
			l(datas, nil)
		}

	case helix.LiveInstanceChanged:
		liveInstances, values, err := cb.conn.ChildrenValues(cb.kb.liveInstances())
		if err != nil {
			log.Error(err)
			return
		}

		datas := make([]*model.LiveInstance, 0, len(liveInstances))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				log.Error(err)
				return
			}

			datas = append(datas, model.NewLiveInstanceFromRecord(record))
		}

		if l, ok := cb.listener.(helix.LiveInstanceChangeListener); ok {
			l(datas, nil)
		}

	case helix.CurrentStateChanged:
		resources, values, err := cb.conn.ChildrenValues(cb.kb.currentStatesForSession(cb.instanceID, cb.SessionID()))
		if err != nil {
			log.Error(err)
			return
		}

		datas := make([]*model.CurrentState, 0, len(resources))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				log.Error(err)
				return
			}

			datas = append(datas, model.NewCurrentStateFromRecord(record))
		}
		if l, ok := cb.listener.(helix.CurrentStateChangeListener); ok {
			l(cb.instanceID, datas, nil)
		}

	case helix.InstanceConfigChanged:
		if l, ok := cb.listener.(helix.InstanceConfigChangeListener); ok {
			data := ctx.ChangeData.([]*model.InstanceConfig)
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
	log.Trace("%s new children %+v", parentPath, currentChilds)

	if !strings.HasPrefix(parentPath, cb.path) {
		return nil
	}

	// TODO if child node deleted, cb.conn.UnsubscribeChildChanges(path, listener)
	cb.invoke(helix.ChangeNotification{
		ChangeType: helix.CallbackInvoke,
	})
	return nil

	/*
		switch cb.changeType {
		case helix.ExternalViewChanged:
			resources, err := cb.conn.Children(parentPath)
			if err != nil {
				return err
			}

			// TODO what if resource deleted
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
			cb.invoke(notify)

		case helix.IdealStateChanged:
			cb.Manager.conn.SubscribeChildChanges(cb.kb.idealStates(), cb)

		case helix.CurrentStateChanged:

		case helix.InstanceConfigChanged:

		case helix.ControllerChanged:

		case helix.ControllerMessagesChanged:

		default:
			return helix.ErrInvalidArgument
		}

		return nil */
}

func (cb *CallbackHandler) HandleDataChange(dataPath string, data []byte) error {
	log.Trace("%s changed to %s", dataPath, string(data))

	if !strings.HasPrefix(dataPath, cb.path) {
		log.Debug(dataPath)
		return nil
	}

	cb.invoke(helix.ChangeNotification{
		ChangeType: helix.CallbackInvoke,
	})

	return nil
}

func (cb *CallbackHandler) HandleDataDeleted(dataPath string) error {
	log.Trace("%s deleted", dataPath)

	if !strings.HasPrefix(dataPath, cb.path) {
		log.Debug(dataPath)
		return nil
	}

	cb.conn.UnsubscribeChildChanges(dataPath, cb)
	cb.conn.UnsubscribeDataChanges(dataPath, cb)

	cb.invoke(helix.ChangeNotification{
		ChangeType: helix.CallbackInvoke,
	})

	return nil
}
