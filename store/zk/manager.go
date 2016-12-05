package zk

import (
	"fmt"
	"sync"

	"github.com/funkygao/go-helix"
)

var _ helix.HelixManager = &Manager{}

type Manager struct {
	sync.RWMutex
	closeOnce sync.Once

	zkSvr     string
	conn      *connection
	connected bool
}

// NewZKHelixManager creates a HelixManager implementation with zk as storage.
func NewZKHelixManager(zkSvr string, options ...zkConnOption) helix.HelixManager {
	mgr := &Manager{
		zkSvr: zkSvr,
		conn:  newConnection(zkSvr),
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

// NewSpectator creates a new Helix Spectator instance. This role handles most "read-only"
// operations of a Helix client.
func (m *Manager) NewSpectator(clusterID string) helix.HelixSpectator {
	return &Spectator{
		ClusterID: clusterID,
		manager:   m,
		conn:      m.conn,
		kb:        keyBuilder{clusterID: clusterID},

		// listeners
		externalViewListeners:       []helix.ExternalViewChangeListener{},
		liveInstanceChangeListeners: []helix.LiveInstanceChangeListener{},
		currentStateChangeListeners: map[string][]helix.CurrentStateChangeListener{},
		messageListeners:            map[string][]helix.MessageListener{},
		idealStateChangeListeners:   []helix.IdealStateChangeListener{},

		// control channels
		stop: make(chan bool),
		externalViewResourceMap: map[string]bool{},
		idealStateResourceMap:   map[string]bool{},
		instanceConfigMap:       map[string]bool{},

		changeNotificationChan: make(chan helix.ChangeNotification, 1000),

		stopCurrentStateWatch: make(map[string]chan interface{}),

		// channel for receiving instance messages
		instanceMessageChannel: make(chan string, 100),
	}
}

// NewParticipant creates a new Helix Participant. This instance will act as a live instance
// of the Helix cluster when connected, and will participate the state model transition.
func (m *Manager) NewParticipant(clusterID string, host string, port string) helix.HelixParticipant {
	return &Participant{
		ClusterID:     clusterID,
		manager:       m,
		conn:          m.conn,
		kb:            keyBuilder{clusterID: clusterID},
		Host:          host,
		Port:          port,
		ParticipantID: fmt.Sprintf("%s_%s", host, port), // node id
		started:       make(chan interface{}),
		stop:          make(chan bool),
	}
}
