package zk

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // pprof runtime
	"sync"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/controller"
	"github.com/funkygao/go-zookeeper/zk"
	"github.com/funkygao/golib/sync2"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/zkclient"
	lru "github.com/hashicorp/golang-lru"
)

var _ helix.HelixManager = &Manager{}
var _ zkclient.ZkStateListener = &Manager{}

type Manager struct {
	sync.RWMutex

	closeOnce sync.Once
	wg        sync.WaitGroup

	zkSvr     string
	conn      *connection
	connected sync2.AtomicBool
	stop      chan struct{}

	clusterID           string
	it                  helix.InstanceType
	kb                  keyBuilder
	pprofPort           int
	preConnectCallbacks []helix.PreConnectCallback

	// only participant holds a state machine engine
	sme        *stateMachineEngine
	timerTasks []helix.HelixTimerTask

	// ClusterManagementTool cache
	admin helix.HelixAdmin

	// GenericHelixController cache
	controller *controller.GenericHelixController

	// ClusterMessagingService cache
	messaging *zkMessagingService

	metrics *metricsReporter

	// host of this participant
	host string

	// port of this participant
	port string

	// instanceID is the optional identifier of this participant, by default to host_port
	instanceID string

	// context of the specator, accessible from the ExternalViewChangeListener
	context *helix.Context

	externalViewListeners       []helix.ExternalViewChangeListener
	liveInstanceChangeListeners []helix.LiveInstanceChangeListener
	idealStateChangeListeners   []helix.IdealStateChangeListener
	currentStateChangeListeners map[string][]helix.CurrentStateChangeListener // key is instance
	messageListeners            map[string][]helix.MessageListener            // key is instance

	// not implemented
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

// NewZkHelixManager creates a HelixManager implementation with zk as storage.
func NewZkHelixManager(clusterID, host, port, zkSvr string,
	it helix.InstanceType, options ...managerOption) (mgr *Manager, err error) {
	mgr = &Manager{
		zkSvr:               zkSvr,
		clusterID:           clusterID,
		conn:                newConnection(zkSvr),
		stop:                make(chan struct{}),
		metrics:             newMetricsReporter(),
		it:                  it,
		kb:                  keyBuilder{clusterID: clusterID},
		preConnectCallbacks: []helix.PreConnectCallback{},
		pprofPort:           10001,
		host:                host,
		port:                port,
		instanceID:          fmt.Sprintf("%s_%s", host, port), // node id

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

	if mgr.receivedMessages, err = lru.New(10 << 10); err != nil {
		return
	}
	mgr.messaging = newZkMessagingService(mgr)

	// apply additional options over the default
	for _, option := range options {
		option(mgr)
	}

	switch it {
	case helix.InstanceTypeParticipant:
		mgr.sme = newStateMachineEngine(mgr)
		mgr.timerTasks = []helix.HelixTimerTask{}
		// ParticipantHealthReportTask

	case helix.InstanceTypeSpectator:

	case helix.InstanceTypeControllerStandalone, helix.InstanceTypeControllerDistributed:
		err = helix.ErrNotImplemented

	default:
		err = helix.ErrNotImplemented
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

	log.Trace("manager{cluster:%s, instance:%s, type:%s, zk:%s} connecting...",
		m.clusterID, m.instanceID, m.it, m.zkSvr)

	if m.pprofPort > 0 {
		addr := fmt.Sprintf("localhost:%d", m.pprofPort)
		go http.ListenAndServe(addr, nil)
		log.Trace("pprof ready on http://%s/debug/pprof", addr)
	}

	if m.it.IsControllerStandalone() || m.it.IsControllerDistributed() {
		if m.controller == nil {
			m.controller = controller.NewGenericHelixController()
		}
	}

	if err := m.connectToZookeeper(); err != nil {
		return err
	}

	m.messaging.onConnected()

	log.Debug("zk connected")

	if ok, err := m.conn.IsClusterSetup(m.clusterID); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	m.connected.Set(true)
	log.Trace("manager{cluster:%s, instance:%s, type:%s, zk:%s} connected",
		m.clusterID, m.instanceID, m.it, m.zkSvr)

	return nil
}

func (m *Manager) Disconnect() {
	m.closeOnce.Do(func() {
		m.Lock()
		if m.connected.Get() {
			m.conn.Disconnect()
			m.connected.Set(false)
		}
		m.Unlock()
	})
}

func (m *Manager) shortID() string {
	return fmt.Sprintf("%s[%s/%s@%s]", m.it, m.instanceID, m.conn.SessionID(), m.clusterID)
}

func (m *Manager) IsConnected() bool {
	return m.connected.Get()
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

func (m *Manager) HandleStateChanged(state zk.State) (err error) {
	log.Debug("new state: %s", state)

	m.connected.Set(false)

	// StateHasSession will be handled by HandleNewSession
	switch state {
	case zk.StateConnecting:

	case zk.StateConnected:

	case zk.StateDisconnected:

	case zk.StateExpired:

	case zk.StateUnknown:
		// e,g. EventNodeChildrenChanged

	}

	return
}

func (m *Manager) HandleNewSession() (err error) {
	log.Trace("%s handling new session", m.shortID())

	if err = m.conn.WaitUntilConnected(0); err != nil {
		return
	}

	m.connected.Set(true)

	switch m.it {
	case helix.InstanceTypeParticipant:
		err = m.handleNewSessionAsParticipant()

	case helix.InstanceTypeSpectator:

	case helix.InstanceTypeControllerStandalone, helix.InstanceTypeControllerDistributed:
		return helix.ErrNotImplemented
	}

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

	// invoke preconnection callbacks
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
	return helix.ErrNotImplemented
}
