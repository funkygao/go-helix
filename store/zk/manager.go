package zk

import (
	"fmt"

	"github.com/funkygao/go-helix"
)

var _ helix.HelixManager = &Manager{}

// The Helix manager is a common component that connects each system component with the controller.
type Manager struct {
	zkSvr string
	conn  *connection
}

// NewManager creates a new instance of Manager from a zookeeper connection string
func NewManager(zkSvr string) helix.HelixManager {
	return &Manager{
		zkSvr: zkSvr,
	}
}

// NewSpectator creates a new Helix Spectator instance. This role handles most "read-only"
// operations of a Helix client.
func (m *Manager) NewSpectator(clusterID string) helix.HelixSpectator {
	return &Spectator{
		ClusterID: clusterID,
		zkSvr:     m.zkSvr,
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
		Host:          host,
		Port:          port,
		ParticipantID: fmt.Sprintf("%s_%s", host, port), // node id
		zkSvr:         m.zkSvr,
		started:       make(chan interface{}),
		stop:          make(chan bool),
		stopWatch:     make(chan bool),
		kb:            keyBuilder{clusterID: clusterID},
	}
}
