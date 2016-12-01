package helix

// HelixManager is a common component that connects each system component with the controller.
type HelixManager interface {

	// Connect the participant to storage and start housekeeping.
	Connect() error

	// Close will disconnect the participant from storage and Helix controller.
	Close()

	// NewSpectator creates a new HelixSpectator instance.
	// This role handles most "read-only" operations of a Helix client.
	NewSpectator(clusterID string) HelixSpectator

	// NewParticipant creates a new HelixParticipant instance.
	// This instance will act as a live instance of the Helix cluster when connected, and
	// will participate the state model transition.
	NewParticipant(clusterID string, host string, port string) HelixParticipant
}
