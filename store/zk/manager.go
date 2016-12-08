package zk

import (
	"fmt"
	"sync"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/golib/sync2"
	log "github.com/funkygao/log4go"
	lru "github.com/hashicorp/golang-lru"
	"github.com/yichen/go-zookeeper/zk"
)

var _ helix.HelixManager = &Manager{}

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
	preConnectCallbacks []helix.PreConnectCallback

	// only participant holds a state machine engine
	sme        *stateMachineEngine
	timerTasks []helix.HelixTimerTask

	// ClusterManagementTool cache
	admin helix.HelixAdmin

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
	currentStateChangeListeners map[string][]helix.CurrentStateChangeListener
	idealStateChangeListeners   []helix.IdealStateChangeListener
	messageListeners            map[string][]helix.MessageListener // key is instance

	// not implemented
	controllerMessageListeners    []helix.ControllerMessageListener
	instanceConfigChangeListeners []helix.InstanceConfigChangeListener

	// resources the external view is tracking.
	// It is a map from the resource name to the current state of the resource:
	// true means it is active, false means the resource is inactive/deleted
	externalViewResourceMap map[string]bool
	idealStateResourceMap   map[string]bool
	instanceConfigMap       map[string]bool

	// changeNotification is a channel to notify any changes that needs to trigger a listener
	changeNotificationChan chan helix.ChangeNotification

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
	it helix.InstanceType, options ...managerOption) helix.HelixManager {
	mgr := &Manager{
		zkSvr:               zkSvr,
		clusterID:           clusterID,
		conn:                newConnection(zkSvr),
		stop:                make(chan struct{}),
		metrics:             newMetricsReporter(),
		it:                  it,
		kb:                  keyBuilder{clusterID: clusterID},
		preConnectCallbacks: []helix.PreConnectCallback{},
		host:                host,
		port:                port,
		instanceID:          fmt.Sprintf("%s_%s", host, port), // node id

		// listeners
		externalViewListeners:       []helix.ExternalViewChangeListener{},
		liveInstanceChangeListeners: []helix.LiveInstanceChangeListener{},
		currentStateChangeListeners: map[string][]helix.CurrentStateChangeListener{},
		idealStateChangeListeners:   []helix.IdealStateChangeListener{},
		messageListeners:            map[string][]helix.MessageListener{},

		// control channels
		externalViewResourceMap: map[string]bool{},
		idealStateResourceMap:   map[string]bool{},
		instanceConfigMap:       map[string]bool{},

		changeNotificationChan: make(chan helix.ChangeNotification, 1000),

		stopCurrentStateWatch: make(map[string]chan interface{}),

		// channel for receiving instance messages
		instanceMessageChannel: make(chan string, 100),
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

	case helix.InstanceTypeController:
		panic(helix.ErrNotImplemented)

	case helix.InstanceTypeNotImplemented:
		panic(helix.ErrNotImplemented)

	default:
		panic("unknown instance type: " + it)
	}

	return mgr
}

func (m *Manager) Connect() error {
	m.RLock()
	if m.connected.Get() {
		m.RUnlock()
		return nil
	}
	m.RUnlock()

	m.Lock()
	defer m.Unlock()
	if m.connected.Get() {
		return nil
	}

	log.Trace("manager{cluster:%s, instance:%s, type:%s, zk:%s} connecting...",
		m.clusterID, m.instanceID, m.it, m.zkSvr)

	if err := m.createZkConnection(); err != nil {
		return err
	}

	// handleNewSessionAsController, handleNewSessionAsParticipant
	// handleStateChanged

	if ok, err := m.conn.IsClusterSetup(m.clusterID); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	m.connected.Set(true)
	return nil
}

func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.Lock()
		if m.connected.Get() {
			m.conn.Disconnect()
			m.connected.Set(false)
		}
		m.Unlock()
	})
}

func (m *Manager) isConnected() bool {
	return m.connected.Get()
}

func (m *Manager) createZkConnection() error {
	if err := m.conn.Connect(); err != nil {
		return err
	}

	m.conn.SubscribeStateChanges(m)
	for retries := 0; retries < 3; retries++ {
		if err := m.conn.waitUntilConnected(); err != nil {
			return err
		}

		if err := m.HandleStateChanged(zk.StateSyncConnected); err != nil {
			return err
		}
		if err := m.HandleNewSession(); err != nil {
			return err
		}
	}

	return nil
}
