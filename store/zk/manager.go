package zk

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // pprof runtime for debugging
	"sync"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/controller"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/go-helix/store/zk/healthcheck"
	"github.com/funkygao/go-helix/ver"
	"github.com/funkygao/go-zookeeper/zk"
	"github.com/funkygao/golib/sync2"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/zkclient"
)

var (
	_ helix.HelixManager       = &Manager{}
	_ zkclient.ZkStateListener = &Manager{}
)

type Manager struct {
	sync.RWMutex

	wg sync.WaitGroup

	conn      *connection
	connected sync2.AtomicBool
	stop      chan struct{}

	clusterID            string
	it                   helix.InstanceType
	kb                   keyBuilder
	pprofPort            int
	preConnectCallbacks  []helix.PreConnectCallback
	postConnectCallbacks []helix.PostConnectCallback

	// participant fields
	sme        *stateMachineEngine
	timerTasks []helix.HelixTimerTask

	// controller fields
	controller           *controller.GenericHelixController
	controllerTimerTasks []helix.HelixTimerTask

	// ClusterManagementTool cache
	admin helix.HelixAdmin

	// ClusterMessagingService cache
	messaging *zkMessagingService

	// metrics reporter
	metrics *metricsReporter

	// identification of the instance
	host, port, instanceID string

	// context of the specator, accessible from the ExternalViewChangeListener
	context *helix.Context // TODO

	handlers                []*CallbackHandler
	cacheLock               sync.RWMutex
	externalViewResourceMap map[string]bool // key is resource
	idealStateResourceMap   map[string]bool // key is resource
	instanceConfigMap       map[string]bool // key is instance
}

// newZkHelixManager creates a HelixManager implementation with zk being storage.
func newZkHelixManager(clusterID, host, port, zkSvr string,
	it helix.InstanceType, options ...ManagerOption) (mgr *Manager, err error) {
	mgr = &Manager{
		clusterID:  clusterID,
		pprofPort:  10001, // TODO
		it:         it,
		host:       host,
		port:       port,
		instanceID: fmt.Sprintf("%s_%s", host, port),

		conn: newConnection(zkSvr),
		stop: make(chan struct{}),
		kb:   newKeyBuilder(clusterID),

		preConnectCallbacks:  []helix.PreConnectCallback{},
		postConnectCallbacks: []helix.PostConnectCallback{},
		metrics:              newMetricsReporter(),

		handlers:                []*CallbackHandler{},
		externalViewResourceMap: map[string]bool{},
		idealStateResourceMap:   map[string]bool{},
		instanceConfigMap:       map[string]bool{},
	}

	if mgr.instanceID == "_" {
		ip, err := ctx.LocalIP()
		if err != nil {
			return nil, err
		}

		mgr.host = ip.String()
		mgr.instanceID = fmt.Sprintf("%s-%s", mgr.host, mgr.it)
	}

	if !mgr.Valid() {
		return nil, helix.ErrInvalidArgument
	}

	// all instance type need messaging service
	mgr.messaging = newZkMessagingService(mgr)
	if mgr.messaging == nil {
		return nil, helix.ErrSystem
	}

	// apply additional options over the default
	for _, option := range options {
		option(mgr)
	}

	switch it {
	case helix.InstanceTypeParticipant:
		mgr.sme = newStateMachineEngine(mgr)
		mgr.timerTasks = []helix.HelixTimerTask{healthcheck.NewParticipanthealthcheckTask()}

	case helix.InstanceTypeControllerDistributed:
		mgr.sme = newStateMachineEngine(mgr)
		err = helix.ErrNotImplemented

	case helix.InstanceTypeControllerStandalone:
		mgr.controllerTimerTasks = []helix.HelixTimerTask{}
		err = helix.ErrNotImplemented

	case helix.InstanceTypeAdministrator, helix.InstanceTypeSpectator:
		// do nothing

	default:
		err = helix.ErrInvalidArgument
	}

	return
}

func (m *Manager) Connect() error {
	m.RLock()
	if m.IsConnected() {
		m.RUnlock()
		return nil
	}
	m.RUnlock()

	m.Lock()
	defer m.Unlock()
	if m.IsConnected() {
		return nil
	}

	t1 := time.Now()
	log.Trace("%s connecting...", m.shortID())

	if m.pprofPort > 0 {
		addr := fmt.Sprintf("localhost:%d", m.pprofPort)
		go http.ListenAndServe(addr, nil)
		log.Info("pprof ready on http://%s/debug/pprof", addr)
	}

	if m.it.IsController() {
		if m.controller == nil {
			m.controller = controller.NewGenericHelixController()
		}
	}

	if err := m.connectToZookeeper(); err != nil {
		return err
	}

	m.wg.Add(1)
	go m.handleListenerErrors()

	m.messaging.onConnected() // if participant, call twice

	if ok, err := m.conn.IsClusterSetup(m.clusterID); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	m.connected.Set(true)
	log.Info("%s connected in %s", m.shortID(), time.Since(t1))

	return nil
}

func (m *Manager) Disconnect() {
	t1 := time.Now()
	log.Trace("%s disconnecting...", m.shortID())

	close(m.stop)
	m.wg.Wait()

	switch m.it {
	case helix.InstanceTypeParticipant:
		log.Trace("%s removed current states with err=%v", m.shortID(),
			m.conn.DeleteTree(m.kb.currentStatesForSession(m.instanceID, m.SessionID())))
	}

	m.conn.Disconnect()
	m.conn = nil
	m.connected.Set(false)

	log.Info("%s disconnected in %s", m.shortID(), time.Since(t1))
}

func (m *Manager) connectToZookeeper() (err error) {
	m.conn.SubscribeStateChanges(m)

	if err = m.conn.Connect(); err != nil {
		return
	}

	for retries := 0; retries < 3; retries++ {
		if err = m.conn.WaitUntilConnected(m.conn.SessionTimeout()); err == nil {
			break
		}

		log.Warn("%s retry=%d %v", m.shortID(), retries, err)
	}

	return
}

func (m *Manager) shortID() string {
	if m.IsConnected() {
		return fmt.Sprintf("%s[%s/%s@%s]", m.it, m.instanceID, m.conn.SessionID(), m.clusterID)
	}

	return fmt.Sprintf("%s[%s/-@%s]", m.it, m.instanceID, m.clusterID)
}

func (m *Manager) IsConnected() bool {
	return m.connected.Get()
}

func (m *Manager) HandleStateChanged(state zk.State) (err error) {
	if state == zk.StateUnknown {
		// e,g. EventNodeChildrenChanged
		return
	}

	log.Debug("%s new state: %s", m.shortID(), state)

	switch state {
	case zk.StateConnecting:
		m.connected.Set(false)
	case zk.StateConnected:
		m.connected.Set(false)
	case zk.StateConnectedReadOnly:
	case zk.StateDisconnected:
		m.connected.Set(false)
	case zk.StateExpired:
		m.connected.Set(false)
	case zk.StateHasSession:
		// StateHasSession will be handled by HandleNewSession
	}

	return
}

func (m *Manager) HandleNewSession() (err error) {
	log.Trace("%s handling new session", m.shortID())

	// TODO lock here

	if err = m.conn.WaitUntilConnected(0); err != nil {
		return
	}

	m.connected.Set(true)

	m.stopTimerTasks()
	m.resetHandlers()

	if ok, err := m.conn.IsClusterSetup(m.clusterID); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	switch m.it {
	case helix.InstanceTypeParticipant:
		err = m.handleNewSessionAsParticipant()

	case helix.InstanceTypeControllerDistributed:
		if err = m.handleNewSessionAsParticipant(); err != nil {
			return
		}
		err = m.handleNewSessionAsController()

	case helix.InstanceTypeControllerStandalone:
		err = m.handleNewSessionAsController()

	case helix.InstanceTypeSpectator:
		err = m.handleNewSessionAsSpectator()

	case helix.InstanceTypeAdministrator:
	}

	if errs := m.startTimerTasks(); len(errs) > 0 {
		for _, e := range errs {
			log.Error("%s timer task %v", m.shortID(), e)
		}
	}

	m.initHandlers()
	return
}

func (m *Manager) handleNewSessionAsParticipant() error {
	t1 := time.Now()

	p := newParticipant(m)
	if ok, err := p.joinCluster(); !ok || err != nil {
		if err != nil {
			return err
		}
		return helix.ErrEnsureParticipantConfig
	}

	// only participant has pre-connect callbacks
	for _, cb := range m.preConnectCallbacks {
		cb()
	}

	if err := p.createLiveInstance(); err != nil {
		return err
	}

	if err := p.carryOverPreviousCurrentState(); err != nil {
		return err
	}

	p.setupMsgHandler()

	// only participant has post-connect callbacks
	for _, cb := range m.postConnectCallbacks {
		cb()
	}

	log.Trace("%s handle new session as participant in %s", p.shortID(), time.Since(t1))

	return nil
}

func (m *Manager) handleNewSessionAsController() error {
	// DistributedLeaderElection
	return helix.ErrNotImplemented
}

func (m *Manager) handleNewSessionAsSpectator() error {
	t1 := time.Now()
	record := model.NewRecord(m.instanceID)
	record.SetStringField("HELIX_VERSION", ver.Ver)
	err := m.conn.CreateLiveNode(m.kb.liveSpectator(m.instanceID), record.Marshal(), 3)
	log.Trace("handle new session as spectator in %s", time.Since(t1))
	return err
}
