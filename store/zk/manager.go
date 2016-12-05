package zk

import (
	"fmt"
	"sync"

	"github.com/funkygao/go-helix"
	lru "github.com/hashicorp/golang-lru"
)

type Manager struct {
	sync.RWMutex
	closeOnce sync.Once

	zkSvr     string
	conn      *connection
	connected bool

	stop chan bool

	kb keyBuilder

	// context of the specator, accessible from the ExternalViewChangeListener
	context *helix.Context

	// external view change handler
	externalViewListeners         []helix.ExternalViewChangeListener
	liveInstanceChangeListeners   []helix.LiveInstanceChangeListener
	currentStateChangeListeners   map[string][]helix.CurrentStateChangeListener
	idealStateChangeListeners     []helix.IdealStateChangeListener
	instanceConfigChangeListeners []helix.InstanceConfigChangeListener
	controllerMessageListeners    []helix.ControllerMessageListener
	messageListeners              map[string][]helix.MessageListener

	// resources the external view is tracking. It is a map from the resource name to the
	// current state of the resource: true means it is active, false means the resource is inactive/deleted
	externalViewResourceMap map[string]bool
	idealStateResourceMap   map[string]bool
	instanceConfigMap       map[string]bool

	// changeNotification is a channel to notify any changes that needs to trigger a listener
	changeNotificationChan chan helix.ChangeNotification

	// instance message channel. Each item in the channel is the instance name that has new messages
	instanceMessageChannel chan string

	// a LRU cache of recently received message IDs. Use this to detect new messages and existing messages
	receivedMessages *lru.Cache

	// control channels for stopping watches
	stopCurrentStateWatch map[string]chan interface{}
}

// NewZKHelixManager creates a HelixManager implementation with zk as storage.
func NewZKHelixManager(zkSvr string, options ...zkConnOption) helix.HelixManager {
	mgr := &Manager{
		zkSvr: zkSvr,
		conn:  newConnection(zkSvr),
		stop:  make(chan bool),

		// listeners
		externalViewListeners:       []helix.ExternalViewChangeListener{},
		liveInstanceChangeListeners: []helix.LiveInstanceChangeListener{},
		currentStateChangeListeners: map[string][]helix.CurrentStateChangeListener{},
		messageListeners:            map[string][]helix.MessageListener{},
		idealStateChangeListeners:   []helix.IdealStateChangeListener{},

		// control channels
		externalViewResourceMap: map[string]bool{},
		idealStateResourceMap:   map[string]bool{},
		instanceConfigMap:       map[string]bool{},

		changeNotificationChan: make(chan helix.ChangeNotification, 1000),

		stopCurrentStateWatch: make(map[string]chan interface{}),

		// channel for receiving instance messages
		instanceMessageChannel: make(chan string, 100),
	}
	for _, option := range options {
		option(mgr.conn)
	}
	return mgr
}

func (m *Manager) Connect() error {
	m.RLock()
	if m.connected {
		m.RUnlock()
		return nil
	}
	m.RUnlock()

	m.Lock()
	defer m.Unlock()
	if m.connected {
		return nil
	}

	if err := m.conn.Connect(); err != nil {
		return err
	}

	m.connected = true
	return nil
}

func (m *Manager) IsConnected() bool {
	return true
}

func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.Lock()
		if m.connected {
			m.conn.Disconnect()
			m.connected = false
		}
		m.Unlock()
	})
}

func (m *Manager) SetContext(context *helix.Context) {
	m.Lock()
	m.context = context
	m.Unlock()
}

// NewSpectator creates a new Helix Spectator instance. This role handles most "read-only"
// operations of a Helix client.
func (m *Manager) NewSpectator(clusterID string) helix.HelixSpectator {
	return &Spectator{
		ClusterID: clusterID,
		Manager:   m,
		conn:      m.conn,
		kb:        keyBuilder{clusterID: clusterID},
	}
}

// NewParticipant creates a new Helix Participant. This instance will act as a live instance
// of the Helix cluster when connected, and will participate the state model transition.
func (m *Manager) NewParticipant(clusterID string, host string, port string) helix.HelixParticipant {
	return &Participant{
		ClusterID:     clusterID,
		Manager:       m,
		conn:          m.conn,
		kb:            keyBuilder{clusterID: clusterID},
		Host:          host,
		Port:          port,
		ParticipantID: fmt.Sprintf("%s_%s", host, port), // node id
		started:       make(chan interface{}),
		stop:          make(chan bool),
	}
}
