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
	"github.com/funkygao/go-zookeeper/zk"
	"github.com/funkygao/golib/sync2"
	log "github.com/funkygao/log4go"
	"github.com/yichen/retry"
)

var (
	zkRetryOptions = retry.RetryOptions{
		"zookeeper",          // tag
		time.Millisecond * 5, // backoff
		time.Second * 1,      // max backoff
		1,                    // default backoff constant
		1,                    // MaxAttempts, 0 means infinite
		false,                // use V(1) level for log messages
	}
)

type ZkStateListener interface {
	HandleStateChanged(zk.State) error

	HandleNewSession() error
}

type connection struct {
	sync.RWMutex

	zkSvr, chroot  string
	servers        []string
	sessionTimeout time.Duration

	isConnected sync2.AtomicBool
	close       chan struct{}
	wg          sync.WaitGroup

	zkConn     *zk.Conn
	stat       *zk.Stat // storage for the lastest zk query stat info FIXME
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
		zkSvr:                zkSvr,
		chroot:               chroot,
		servers:              servers,
		close:                make(chan struct{}),
		sessionTimeout:       time.Second * 30,
		stateChangeListeners: []ZkStateListener{},
	}
	conn.isConnected.Set(false)

	return &conn
}

func (conn *connection) Connect() error {
	t1 := time.Now()
	zkConn, stateEvtCh, err := zk.Connect(conn.servers, conn.sessionTimeout)
	if err != nil {
		return err
	}

	conn.zkConn = zkConn
	conn.stateEvtCh = stateEvtCh

	if conn.chroot != "" {
		if err := conn.ensurePathExists(conn.chroot); err != nil {
			return err
		}
	}

	conn.wg.Add(1)
	go conn.watchStateChanges()

	log.Debug("zk connection Connect %s", time.Since(t1))

	return nil
}

func (conn *connection) Disconnect() {
	t1 := time.Now()
	if conn.zkConn != nil {
		conn.zkConn.Close()
	}
	close(conn.close)
	conn.wg.Wait()
	conn.isConnected.Set(false)

	log.Debug("zk connection Disconnect %s", time.Since(t1))
}

// SubscribeStateChanges MUST be called before Connect as we don't want
// to labor to handle the thread-safe issue.
func (conn *connection) SubscribeStateChanges(l ZkStateListener) {
	conn.stateChangeListeners = append(conn.stateChangeListeners, l)
}

func (conn *connection) watchStateChanges() {
	defer conn.wg.Done()

	var evt zk.Event
	for {
		select {
		case <-conn.close:
			log.Debug("zk connection got close signal, stopped ok")
			return

		case evt = <-conn.stateEvtCh:
			// TODO lock? currently, SubscribeStateChanges must called before Connect
			// what if handler blocks?
			for _, l := range conn.stateChangeListeners {
				l.HandleStateChanged(evt.State)
			}

			// extra handler for new session state
			if evt.State == zk.StateHasSession {
				conn.isConnected.Set(true)
				for _, l := range conn.stateChangeListeners {
					l.HandleNewSession()
				}
			} else if evt.State != zk.StateUnknown {
				conn.isConnected.Set(false)
			}
		}
	}
}

func (conn connection) realPath(path string) string {
	if conn.chroot == "" {
		return path
	}

	return strings.TrimRight(conn.chroot+path, "/")
}

func (conn *connection) waitUntilConnected(d time.Duration) (err error) {
	t1 := time.Now()
	retries := 0
	for {
		if _, _, err = conn.zkConn.Exists("/zookeeper"); err == nil {
			break
		}

		retries++
		log.Debug("waitUntilConnected: retry=%d %v", retries, err)

		if d > 0 && time.Since(t1) > d {
			break
		} else if d > 0 {
			time.Sleep(d)
		} else {
			time.Sleep(conn.sessionTimeout)
		}
	}

	log.Debug("zk connection waitUntilConnected %s", time.Since(t1))
	return
}

func (conn *connection) IsConnected() bool {
	return conn != nil && conn.isConnected.Get()
}

func (conn *connection) SessionID() string {
	return strconv.FormatInt(conn.zkConn.SessionID(), 10)
}

func (conn *connection) CreatePersistentRecord(p string, r *model.Record) error {
	parent := path.Dir(p)
	err := conn.ensurePathExists(conn.realPath(parent))
	if err != nil {
		return err
	}

	return conn.CreatePersistent(p, r.Marshal())
}

func (conn *connection) GetRecord(path string) (*model.Record, error) {
	data, err := conn.Get(path)
	if err != nil {
		return nil, err
	}
	return model.NewRecordFromBytes(data)
}

func (conn *connection) SetRecord(path string, r *model.Record) error {
	exists, err := conn.Exists(path)
	if err != nil {
		return err
	}

	if !exists {
		if err = conn.ensurePathExists(conn.realPath(path)); err != nil {
			return err
		}
	}

	if _, err = conn.Get(path); err != nil {
		return err
	}

	return conn.Set(path, r.Marshal())
}

func (conn *connection) RemoveMapFieldKey(path string, key string) error {
	data, err := conn.Get(path)
	if err != nil {
		return err
	}

	record, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	record.RemoveMapField(key)
	return conn.Set(path, record.Marshal())
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
	data, err := conn.Get(path)
	if err != nil {
		return err
	}

	// convert the result into Record
	record, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	record.SetMapField(key, property, value)
	return conn.Set(path, record.Marshal())
}

// TODO
func (conn *connection) UpdateListField(path string, key string) error {
	return nil
}

func (conn *connection) UpdateSimpleField(path string, key string, value string) error {
	data, err := conn.Get(path)
	if err != nil {
		return err
	}

	// convert the result into Record
	record, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	record.SetSimpleField(key, value)
	return conn.Set(path, record.Marshal())
}

func (conn *connection) GetSimpleFieldValueByKey(path string, key string) (string, error) {
	data, err := conn.Get(path)
	if err != nil {
		return "", err
	}

	record, err := model.NewRecordFromBytes(data)
	if err != nil {
		return "", err
	}

	v := record.GetSimpleField(key)
	if v == nil {
		return "", nil
	}
	return v.(string), nil
}

func (conn *connection) GetSimpleFieldBool(path string, key string) bool {
	result, _ := conn.GetSimpleFieldValueByKey(path, key)
	return strings.ToUpper(result) == "TRUE"
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

		// ok
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

		// ok
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

		// ok
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

func (conn *connection) CreatePersistent(path string, data []byte) error {
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)
	_, err := conn.Create(path, data, flags, acl)
	return err
}

func (conn *connection) CreateEphemeral(path string, data []byte) error {
	flags := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)
	_, err := conn.Create(path, data, flags, acl)
	return err
}

func (conn *connection) CreateEmptyPersistent(path string) error {
	return conn.CreatePersistent(path, []byte{})
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

		// ok
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

	flags := int32(0)
	conn.zkConn.Create(p, []byte{}, flags, zk.WorldACL(zk.PermAll))
	return nil
}

func (conn *connection) wrapZkError(path string, err error) error {
	return fmt.Errorf("%s %v", path, err)
}
