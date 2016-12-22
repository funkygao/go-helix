package zk

import (
	"fmt"
	gopath "path"
	"strings"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
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
	watchChild bool
}

func newCallbackHandler(mgr *Manager, path string, listener interface{},
	changeType helix.ChangeNotificationType, watchChild bool) *CallbackHandler {
	return &CallbackHandler{
		Manager:    mgr,
		listener:   listener,
		path:       path,
		changeType: changeType,
		watchChild: watchChild,
	}
}

func (cb *CallbackHandler) String() string {
	return fmt.Sprintf("%s^%s", cb.path, cb.changeType)
}

func (cb *CallbackHandler) Init() {
	cb.invoke(helix.ChangeNotification{
		ChangeType: helix.CallbackInit,
	})
}

func (cb *CallbackHandler) Reset() {
	cb.invoke(helix.ChangeNotification{
		ChangeType: helix.CallbackFinalize,
	})
}

func (cb *CallbackHandler) subscribeDataChanges(path string, ctx helix.ChangeNotification) {
	switch ctx.ChangeType {
	case helix.CallbackInit:
		cb.conn.SubscribeDataChanges(path, cb)

	case helix.CallbackInvoke:

	case helix.CallbackFinalize:
		cb.conn.UnsubscribeDataChanges(path, cb)
	}
}

func (cb *CallbackHandler) subscribeChildChanges(path string, ctx helix.ChangeNotification) {
	switch ctx.ChangeType {
	case helix.CallbackInit:
		cb.conn.SubscribeChildChanges(path, cb)

	case helix.CallbackInvoke:

	case helix.CallbackFinalize:
		cb.conn.UnsubscribeChildChanges(path, cb)
	}
}

func (cb *CallbackHandler) subscribeForChanges(path string, ctx helix.ChangeNotification) {
	cb.subscribeChildChanges(path, ctx)

	if !cb.watchChild {
		return
	}

	switch cb.changeType {
	case helix.ExternalViewChanged, helix.IdealStateChanged, helix.CurrentStateChanged:
		children, err := cb.conn.Children(path)
		if err != nil {
			log.Error("%s %s %v %s", cb, path, err, ctx)
			return
		}

		for _, child := range children {
			childPath := gopath.Join(path, child)
			data, err := cb.conn.Get(childPath)
			if err != nil {
				log.Error("%s %s %v %s", cb, childPath, err, ctx)
				continue
			}

			record, err := model.NewRecordFromBytes(data)
			if err != nil {
				log.Error("%v data:%s", err, string(data))
				continue
			}

			if record.BucketSize() > 0 {
				// TODO
				// subscribe both data-change and child-change on bucketized parent node
				// data-change gives a delete-callback which is used to remove watch
				cb.subscribeChildChanges(gopath.Join(path, record.ID), ctx)
				cb.subscribeDataChanges(gopath.Join(path, record.ID), ctx)
			} else {
				cb.subscribeDataChanges(gopath.Join(path, record.ID), ctx)
			}
		}

	default:
		children, err := cb.conn.Children(path)
		if err != nil {
			log.Error("%s %s %v %s", cb, path, err, ctx)
			return
		}

		for _, child := range children {
			cb.subscribeDataChanges(gopath.Join(path, child), ctx)
		}
	}
}

// TODO refactor to strip dup code block
func (cb *CallbackHandler) invoke(ctx helix.ChangeNotification) {
	log.Debug("%s %s invoking %s", cb.shortID(), cb, ctx)

	// TODO
	//cb.Manager.Lock()
	//defer cb.Manager.Unlock()

	cb.subscribeForChanges(cb.path, ctx)
	if ctx.ChangeType == helix.CallbackFinalize {
		return
	}

	switch cb.changeType {
	case helix.ExternalViewChanged:
		// TODO what if resource deleted
		resources, values, err := cb.conn.ChildrenValues(cb.path)
		if err != nil {
			log.Error("%s %v", cb.path, err)
			return
		}

		datas := make([]*model.ExternalView, 0, len(resources))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				log.Error("%v %s", err, string(val))
				return
			}

			datas = append(datas, model.NewExternalViewFromRecord(record))
		}

		cb.listener.(helix.ExternalViewChangeListener)(datas, nil)

	case helix.IdealStateChanged:
		resources, values, err := cb.conn.ChildrenValues(cb.path)
		if err != nil {
			log.Error("%s %v", cb.path, err)
			return
		}

		datas := make([]*model.IdealState, 0, len(resources))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				log.Error("%v %s", err, string(val))
				return
			}

			datas = append(datas, model.NewIdealStateFromRecord(record))
		}

		cb.listener.(helix.IdealStateChangeListener)(datas, nil)

	case helix.LiveInstanceChanged:
		liveInstances, values, err := cb.conn.ChildrenValues(cb.path)
		if err != nil {
			log.Error("%s %v", cb.path, err)
			return
		}

		datas := make([]*model.LiveInstance, 0, len(liveInstances))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				log.Error("%v %s", err, string(val))
				return
			}

			datas = append(datas, model.NewLiveInstanceFromRecord(record))
		}

		cb.listener.(helix.LiveInstanceChangeListener)(datas, nil)

	case helix.CurrentStateChanged:
		resources, values, err := cb.conn.ChildrenValues(cb.path)
		if err != nil {
			log.Error("%s %v", cb.path, err)
			return
		}

		datas := make([]*model.CurrentState, 0, len(resources))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				log.Error("%v %s", err, string(val))
				return
			}

			datas = append(datas, model.NewCurrentStateFromRecord(record))
		}

		cb.listener.(helix.CurrentStateChangeListener)(cb.instanceID, datas, nil)

	case helix.InstanceConfigChanged:
		instances, values, err := cb.conn.ChildrenValues(cb.path)
		if err != nil {
			log.Error("%s %v", cb.path, err)
			return
		}

		datas := make([]*model.InstanceConfig, 0, len(instances))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				log.Error("%v %s", err, string(val))
				return
			}

			datas = append(datas, model.NewInstanceConfigFromRecord(record))
		}

		cb.listener.(helix.InstanceConfigChangeListener)(datas, nil)

	case helix.ControllerChanged:
		cb.listener.(helix.ControllerChangeListener)(nil)

	case helix.ControllerMessagesChanged:
		messageIds, values, err := cb.conn.ChildrenValues(cb.path)
		if err != nil {
			log.Error("%s %v", cb.path, err)
			return
		}

		datas := make([]*model.Message, 0, len(messageIds))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				log.Error("%v %s", err, string(val))
				return
			}

			datas = append(datas, model.NewMessageFromRecord(record))
		}

		if len(datas) > 0 {
			cb.listener.(helix.MessageListener)(datas[0].TargetName(), datas, nil)
		}

	case helix.InstanceMessagesChanged:
		messageIds, values, err := cb.conn.ChildrenValues(cb.path)
		if err != nil {
			log.Error("%s %v", cb.path, err)
			return
		}

		datas := make([]*model.Message, 0, len(messageIds))
		for _, val := range values {
			record, err := model.NewRecordFromBytes(val)
			if err != nil {
				log.Error("%v %s", err, string(val))
				return
			}

			datas = append(datas, model.NewMessageFromRecord(record))
		}

		if len(datas) > 0 {
			cb.listener.(helix.MessageListener)(datas[0].TargetName(), datas, nil)
		}
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
}

func (cb *CallbackHandler) HandleDataChange(dataPath string, data []byte) error {
	log.Trace("%s %s data changed", cb, dataPath)

	if !strings.HasPrefix(dataPath, cb.path) {
		return nil
	}

	cb.invoke(helix.ChangeNotification{
		ChangeType: helix.CallbackInvoke,
	})

	return nil
}

func (cb *CallbackHandler) HandleDataDeleted(dataPath string) error {
	log.Trace("%s %s data deleted", cb, dataPath)

	if !strings.HasPrefix(dataPath, cb.path) {
		return nil
	}

	// session expiry will trigger /{cluster}/LIVEINSTANCES/{node} deleted

	cb.conn.UnsubscribeChildChanges(dataPath, cb)
	cb.conn.UnsubscribeDataChanges(dataPath, cb)

	cb.invoke(helix.ChangeNotification{
		ChangeType: helix.CallbackInvoke,
	})

	return nil
}
