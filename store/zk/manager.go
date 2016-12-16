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
	"github.com/funkygao/go-helix/store/zk/healthcheck"
	"github.com/funkygao/go-zookeeper/zk"
	"github.com/funkygao/golib/sync2"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/zkclient"
	lru "github.com/hashicorp/golang-lru"
)

var _ helix.HelixManager = &Manager{}
var _ zkclient.ZkStateListener = &Manager{}

// NewZkParticipant creates a Participant implementation with zk as storage.
func NewZkParticipant(clusterID, host, port, zkSvr string, options ...ManagerOption) (mgr *Manager, err error) {
	return newZkHelixManager(clusterID, host, port, zkSvr, helix.InstanceTypeParticipant, options...)
}

// NewZkSpectator creates a Spectator implementation with zk as storage.
func NewZkSpectator(clusterID, host, port, zkSvr string, options ...ManagerOption) (mgr *Manager, err error) {
	return newZkHelixManager(clusterID, host, port, zkSvr, helix.InstanceTypeSpectator, options...)
}

// NewZkStandaloneController creates a Standalone Controller implementation with zk as storage.
func NewZkStandaloneController(clusterID, host, port, zkSvr string, options ...ManagerOption) (mgr *Manager, err error) {
	return newZkHelixManager(clusterID, host, port, zkSvr, helix.InstanceTypeControllerStandalone, options...)
}

// NewZkDistributedController creates a Distributed Controller implementation with zk as storage.
func NewZkDistributedController(clusterID, host, port, zkSvr string, options ...ManagerOption) (mgr *Manager, err error) {
	return newZkHelixManager(clusterID, host, port, zkSvr, helix.InstanceTypeControllerDistributed, options...)
}

type Manager struct {
	sync.RWMutex

	wg sync.WaitGroup

	conn      *connection
	connected sync2.AtomicBool
	stop      chan struct{}

	clusterID           string
	it                  helix.InstanceType
	kb                  keyBuilder
	pprofPort           int
	preConnectCallbacks []helix.PreConnectCallback

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

	handlers []*CallbackHandler

	externalViewListeners       []helix.ExternalViewChangeListener
	liveInstanceChangeListeners []helix.LiveInstanceChangeListener
	idealStateChangeListeners   []helix.IdealStateChangeListener
	currentStateChangeListeners map[string][]helix.CurrentStateChangeListener // key is instance
	messageListeners            map[string][]helix.MessageListener            // key is instance

	// not implemented TODO
	controllerMessageListeners    []helix.ControllerMessageListener
	instanceConfigChangeListeners []helix.InstanceConfigChangeListener

	// resources the external view is tracking.
	// It is a map from the resource name to the current state of the resource:
	// true means it is active, false means the resource is inactive/deleted
	externalViewResourceMap map[string]bool // key is resource
	idealStateResourceMap   map[string]bool // key is resource
	instanceConfigMap       map[string]bool // key is resource

	// changeNotification is a channel to notify any changes that needs to trigger a listener
	changeNotificationChan    chan helix.ChangeNotification
	changeNotificationErrChan chan error

	// instance message channel.
	// Each item in the channel is the instance name that has new messages
	instanceMessageChannel chan string

	// a LRU cache of recently received message IDs.
	// Use this to detect new messages and existing messages
	receivedMessages *lru.Cache

	// control channels for stopping watches
	stopCurrentStateWatch map[string]chan interface{}
}

// newZkHelixManager creates a HelixManager implementation with zk as storage.
func newZkHelixManager(clusterID, host, port, zkSvr string,
	it helix.InstanceType, options ...ManagerOption) (mgr *Manager, err error) {
	mgr = &Manager{
		clusterID:           clusterID,
		conn:                newConnection(zkSvr),
		stop:                make(chan struct{}),
		metrics:             newMetricsReporter(),
		it:                  it,
		kb:                  newKeyBuilder(clusterID),
		preConnectCallbacks: []helix.PreConnectCallback{},
		handlers:            []*CallbackHandler{},
		pprofPort:           10001,
		host:                host,
		port:                port,
		instanceID:          fmt.Sprintf("%s_%s", host, port),

		// listeners
		externalViewListeners:       []helix.ExternalViewChangeListener{},
		liveInstanceChangeListeners: []helix.LiveInstanceChangeListener{},
		idealStateChangeListeners:   []helix.IdealStateChangeListener{},
		currentStateChangeListeners: map[string][]helix.CurrentStateChangeListener{},
		messageListeners:            map[string][]helix.MessageListener{},

		// control channels
		externalViewResourceMap: map[string]bool{},
		idealStateResourceMap:   map[string]bool{},
		instanceConfigMap:       map[string]bool{},

		changeNotificationChan:    make(chan helix.ChangeNotification, 16),
		changeNotificationErrChan: make(chan error, 10),

		stopCurrentStateWatch: make(map[string]chan interface{}),

		// channel for receiving instance messages
		instanceMessageChannel: make(chan string, 100),
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

	if mgr.receivedMessages, err = lru.New(10 << 10); err != nil {
		return
	}

	// all instance type need messaging service
	mgr.messaging = newZkMessagingService(mgr)

	// apply additional options over the default
	for _, option := range options {
		option(mgr)
	}

	switch it {
	case helix.InstanceTypeParticipant:
		mgr.sme = newStateMachineEngine(mgr)
		mgr.timerTasks = []helix.HelixTimerTask{healthcheck.NewParticipanthealthcheckTask()}

	case helix.InstanceTypeSpectator:
		// do nothing

	case helix.InstanceTypeControllerDistributed:
		mgr.sme = newStateMachineEngine(mgr)
		err = helix.ErrNotImplemented

	case helix.InstanceTypeControllerStandalone:
		mgr.controllerTimerTasks = []helix.HelixTimerTask{}
		err = helix.ErrNotImplemented

	case helix.InstanceTypeAdministrator:

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
	log.Info("%s connecting...", m.shortID())

	if m.pprofPort > 0 {
		addr := fmt.Sprintf("localhost:%d", m.pprofPort)
		go http.ListenAndServe(addr, nil)
		log.Trace("pprof ready on http://%s/debug/pprof", addr)
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
	close(m.stop)
	m.wg.Wait()

	m.conn.Disconnect()

	m.conn = nil
	m.connected.Set(false)

	log.Info("%s disconnected", m.shortID())
}

func (m *Manager) connectToZookeeper() (err error) {
	// will trigger HandleStateChanged, HandleNewSession
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
	log.Debug("new state: %s", state)

	m.connected.Set(false)

	// StateHasSession will be handled by HandleNewSession
	switch state {
	case zk.StateConnecting:
	case zk.StateConnected:
	case zk.StateConnectedReadOnly:
	case zk.StateDisconnected:
	case zk.StateExpired:
	case zk.StateUnknown: // e,g. EventNodeChildrenChanged
	}

	return
}

func (m *Manager) HandleNewSession() (err error) {
	log.Trace("%s handling new session", m.shortID())

	if err = m.conn.WaitUntilConnected(0); err != nil {
		return
	}

	m.connected.Set(true)

	m.stopTimerTasks()
	m.resetHandlers()
	m.stopChangeNotificationLoop()

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
	case helix.InstanceTypeAdministrator:
	}

	if errs := m.startTimerTasks(); len(errs) > 0 {
		for _, e := range errs {
			log.Error("%s %v", m.shortID(), e)
		}
	}

	m.initHandlers()
	m.startChangeNotificationLoop()
	return
}

func (m *Manager) handleNewSessionAsParticipant() error {
	p := newParticipant(m)
	if ok, err := p.joinCluster(); !ok || err != nil {
		if err != nil {
			return err
		}
		return helix.ErrEnsureParticipantConfig
	}

	// only participant has pre connection callbacks
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

	return nil
}

func (m *Manager) handleNewSessionAsController() error {
	// DistributedLeaderElection
	return helix.ErrNotImplemented
}
