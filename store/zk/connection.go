package zk

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/yichen/go-zookeeper/zk"
	"github.com/yichen/retry"
)

var (
	zkRetryOptions = retry.RetryOptions{
		"zookeeper",            // tag
		time.Millisecond * 500, // backoff
		time.Second * 1,        // max backoff
		1,                      // default backoff constant
		0,                      // infinit retry
		false,                  // use V(1) level for log messages
	}
)

type ZkStateListener interface {
	HandleStateChanged(zk.State) error

	HandleNewSession() error
}

type connection struct {
	sync.RWMutex

	sessionTimeout time.Duration
	servers        []string
	chroot         string
	isConnected    bool
	close          chan struct{}

	zkConn     *zk.Conn
	stat       *zk.Stat // storage for the lastest zk query stat info
	stateEvtCh <-chan zk.Event

	stateChangeListeners []ZkStateListener
}

func newConnection(zkSvr string) *connection {
	servers, chroot, err := parseZkConnStr(zkSvr)
	if err != nil || len(servers) == 0 {
		// yes, panic!
		panic("invalid zkSvr")
	}

	conn := connection{
		servers:              servers,
		chroot:               chroot,
		close:                make(chan struct{}),
		sessionTimeout:       time.Second * 30,
		stateChangeListeners: []ZkStateListener{},
	}

	return &conn
}

func (conn *connection) Connect() error {
	zkConn, stateEvtCh, err := zk.Connect(conn.servers, conn.sessionTimeout)
	if err != nil {
		return err
	}

	conn.zkConn = zkConn
	conn.stateEvtCh = stateEvtCh

	if conn.chroot != "" {
		conn.ensurePathExists(conn.chroot)
	}

	go func() {
		for {
			select {
			case <-conn.close:
				return

			case evt := <-conn.stateEvtCh:
				conn.fireStateChangedEvent(evt.State)
				if evt.State == zk.StateHasSession {
					conn.fireNewSessionEvents()
				}
			}
		}
	}()

	conn.isConnected = true
	return nil
}

func (conn *connection) Disconnect() {
	if conn.zkConn != nil {
		conn.zkConn.Close()
	}
	close(conn.close)
	conn.isConnected = false
}

func (conn *connection) SubscribeStateChanges(l ZkStateListener) {
	conn.Lock()
	conn.stateChangeListeners = append(conn.stateChangeListeners, l)
	conn.Unlock()
}

func (conn *connection) fireStateChangedEvent(state zk.State) {
	conn.RLock()
	defer conn.RUnlock()

	for _, l := range conn.stateChangeListeners {
		l.HandleStateChanged(state)
	}
}

func (conn *connection) fireNewSessionEvents() {
	conn.RLock()
	defer conn.RUnlock()

	for _, l := range conn.stateChangeListeners {
		l.HandleNewSession()
	}
}

func (conn connection) realPath(path string) string {
	if conn.chroot == "" {
		return path
	}

	return strings.TrimRight(conn.chroot+path, "/")
}

func (conn *connection) waitUntilConnected() error {
	if _, _, err := conn.zkConn.Exists("/zookeeper"); err != nil {
		return err
	}

	return nil
}

func (conn *connection) IsConnected() bool {
	return conn != nil && conn.isConnected
}

func (conn *connection) GetSessionID() string {
	return strconv.FormatInt(conn.zkConn.SessionID, 10)
}

func (conn *connection) CreateEmptyNode(path string) error {
	return conn.CreateRecordWithData(path, "")
}

func (conn *connection) CreateRecordWithData(path string, data string) error {
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)

	_, err := conn.Create(path, []byte(data), flags, acl)
	return err
}

func (conn *connection) CreateRecordWithPath(p string, r *model.Record) error {
	parent := path.Dir(p)
	conn.ensurePathExists(conn.realPath(parent))

	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)
	_, err := conn.Create(p, r.Marshal(), flags, acl)
	return err
}

func (conn *connection) Exists(path string) (bool, error) {
	if !conn.IsConnected() {
		return false, helix.ErrNotConnected
	}

	var result bool
	var stat *zk.Stat

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		r, s, err := conn.zkConn.Exists(conn.realPath(path))
		if err != nil {
			return retry.RetryContinue, conn.wrapZkError(conn.realPath(path), err)
		}
		result = r
		stat = s
		return retry.RetryBreak, nil
	})

	conn.stat = stat
	return result, err
}

func (conn *connection) ExistsAll(paths ...string) (bool, error) {
	for _, path := range paths {
		if exists, err := conn.Exists(path); err != nil || exists == false {
			return exists, err
		}
	}

	return true, nil
}

func (conn *connection) Get(path string) ([]byte, error) {
	if !conn.IsConnected() {
		return nil, helix.ErrNotConnected
	}

	var data []byte

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		d, s, err := conn.zkConn.Get(conn.realPath(path))
		if err != nil {
			return retry.RetryContinue, conn.wrapZkError(conn.realPath(path), err)
		}
		data = d
		conn.stat = s
		return retry.RetryBreak, nil
	})

	return data, err
}

func (conn *connection) GetW(path string) ([]byte, <-chan zk.Event, error) {
	if !conn.IsConnected() {
		return nil, nil, helix.ErrNotConnected
	}

	var data []byte
	var events <-chan zk.Event

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		d, s, evts, err := conn.zkConn.GetW(conn.realPath(path))
		if err != nil {
			return retry.RetryContinue, conn.wrapZkError(conn.realPath(path), err)
		}
		data = d
		conn.stat = s
		events = evts
		return retry.RetryBreak, nil
	})

	return data, events, err
}

func (conn *connection) Set(path string, data []byte) error {
	if !conn.IsConnected() {
		return helix.ErrNotConnected
	}

	_, err := conn.zkConn.Set(conn.realPath(path), data, conn.stat.Version)
	return err
}

func (conn *connection) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	if !conn.IsConnected() {
		return "", helix.ErrNotConnected
	}

	return conn.zkConn.Create(conn.realPath(path), data, flags, acl)
}

func (conn *connection) Children(path string) ([]string, error) {
	if !conn.IsConnected() {
		return nil, helix.ErrNotConnected
	}

	var children []string
	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		c, s, err := conn.zkConn.Children(conn.realPath(path))
		if err != nil {
			return retry.RetryContinue, conn.wrapZkError(conn.realPath(path), err)
		}
		children = c
		conn.stat = s
		return retry.RetryBreak, nil
	})

	return children, err
}

func (conn *connection) ChildrenW(path string) ([]string, <-chan zk.Event, error) {
	if !conn.IsConnected() {
		return nil, nil, helix.ErrNotConnected
	}

	var children []string
	var eventChan <-chan zk.Event

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		c, s, evts, err := conn.zkConn.ChildrenW(conn.realPath(path))
		if err != nil {
			return retry.RetryContinue, conn.wrapZkError(conn.realPath(path), err)
		}
		children = c
		conn.stat = s
		eventChan = evts
		return retry.RetryBreak, nil
	})

	return children, eventChan, err
}

// update a map field for the znode. path is the znode path. key is the top-level key in
// the MapFields, mapProperty is the inner key, and value is the. For example:
//
// mapFields":{
// "eat1-app993.stg.linkedin.com_11932,BizProfile,p31_1,SLAVE":{
//   "CURRENT_STATE":"ONLINE"
//   ,"INFO":""
// }
// if we want to set the CURRENT_STATE to ONLINE, we call
// UpdateMapField("/RELAY/INSTANCES/{instance}/CURRENT_STATE/{sessionID}/{db}", "eat1-app993.stg.linkedin.com_11932,BizProfile,p31_1,SLAVE", "CURRENT_STATE", "ONLINE")
func (conn *connection) UpdateMapField(path string, key string, property string, value string) error {
	data, err := conn.Get(path) // Get itself handles chroot
	if err != nil {
		return err
	}

	// convert the result into Record
	node, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	// update the value
	node.SetMapField(key, property, value)

	// copy back to zookeeper
	return conn.Set(path, node.Marshal())
}

func (conn *connection) UpdateSimpleField(path string, key string, value string) error {
	// get the current node
	data, err := conn.Get(path)
	if err != nil {
		return err
	}

	// convert the result into Record
	node, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	// update the value
	node.SetSimpleField(key, value)

	// copy back to zookeeper
	return conn.Set(path, node.Marshal())
}

func (conn *connection) GetSimpleFieldValueByKey(path string, key string) string {
	data, err := conn.Get(path)
	must(err) // FIXME

	node, err := model.NewRecordFromBytes(data)
	must(err) // FIXME

	if node.SimpleFields == nil {
		return ""
	}

	v := node.GetSimpleField(key)
	if v == nil {
		return ""
	}
	return v.(string)
}

func (conn *connection) GetSimpleFieldBool(path string, key string) bool {
	result := conn.GetSimpleFieldValueByKey(path, key)
	return strings.ToUpper(result) == "TRUE"
}

func (conn *connection) Delete(path string) error {
	if !conn.IsConnected() {
		return helix.ErrNotConnected
	}

	return conn.zkConn.Delete(conn.realPath(path), -1)
}

func (conn *connection) DeleteTree(path string) error {
	if !conn.IsConnected() {
		return helix.ErrNotConnected
	}

	return conn.deleteTreeRealPath(conn.realPath(path))
}

func (conn *connection) deleteTreeRealPath(path string) error {
	if exists, _, err := conn.zkConn.Exists(path); !exists || err != nil {
		return err
	}

	children, _, err := conn.zkConn.Children(path)
	if err != nil {
		return err
	}

	if len(children) == 0 {
		err := conn.zkConn.Delete(path, -1)
		return err
	}

	for _, c := range children {
		p := path + "/" + c
		e := conn.deleteTreeRealPath(p)
		if e != nil {
			return e
		}
	}

	return conn.zkConn.Delete(path, -1)
}

func (conn *connection) RemoveMapFieldKey(path string, key string) error {
	data, err := conn.Get(path)
	if err != nil {
		return err
	}

	node, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	node.RemoveMapField(key)

	// save the data back to zookeeper
	return conn.Set(path, node.Marshal())
}

func (conn *connection) IsClusterSetup(cluster string) (bool, error) {
	if cluster == "" {
		return false, helix.ErrInvalidClusterName
	}
	if !conn.IsConnected() {
		return false, helix.ErrNotConnected
	}

	kb := keyBuilder{clusterID: cluster}
	return conn.ExistsAll(
		kb.cluster(),
		kb.idealStates(),
		kb.participantConfigs(),
		kb.propertyStore(),
		kb.liveInstances(),
		kb.instances(),
		kb.externalView(),
		kb.stateModelDefs(),
		kb.controller(),
		kb.controllerErrors(),
		kb.controllerHistory(),
		kb.controllerMessages(),
		kb.controllerStatusUpdates(),
	)
}

func (conn *connection) IsInstanceSetup(cluster, node string) (bool, error) {
	if cluster == "" {
		return false, helix.ErrInvalidClusterName
	}
	if !conn.IsConnected() {
		return false, helix.ErrNotConnected
	}

	kb := keyBuilder{clusterID: cluster}
	return conn.ExistsAll(
		kb.participantConfig(node),
		kb.instance(node),
		kb.messages(node),
		kb.currentStates(node),
		kb.errorsR(node),
		kb.statusUpdates(node),
		kb.healthReport(node),
	)
}

func (conn *connection) GetRecordFromPath(path string) (*model.Record, error) {
	data, err := conn.Get(path)
	if err != nil {
		return nil, err
	}
	return model.NewRecordFromBytes(data)
}

func (conn *connection) SetRecordForPath(path string, r *model.Record) error {
	exists, err := conn.Exists(path)
	if err != nil {
		return err
	}

	if !exists {
		conn.ensurePathExists(conn.realPath(path))
	}

	// need to get the stat.version before calling set
	conn.Lock()
	defer conn.Unlock()

	if _, err := conn.Get(path); err != nil {
		return err
	}

	if err := conn.Set(path, r.Marshal()); err != nil {
		return err
	}

	return nil
}

func (conn *connection) ensurePathExists(p string) error {
	if exists, _, _ := conn.zkConn.Exists(p); exists {
		return nil
	}

	parent := path.Dir(p)
	if exists, _, _ := conn.zkConn.Exists(parent); !exists {
		if err := conn.ensurePathExists(parent); err != nil {
			return err
		}
	}

	conn.zkConn.Create(p, []byte{}, 0, zk.WorldACL(zk.PermAll))
	return nil
}

func (conn *connection) wrapZkError(path string, err error) error {
	return fmt.Errorf("%s %v", path, err)
}
