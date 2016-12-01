package helix

// HelixManager is a common component that connects each system component with the controller.
type HelixManager interface {

	// NewSpectator creates a new HelixSpectator instance.
	// This role handles most "read-only" operations of a Helix client.
	NewSpectator(clusterID string) HelixSpectator

	// NewParticipant creates a new HelixParticipant instance.
	// This instance will act as a live instance of the Helix cluster when connected, and
	// will participate the state model transition.
	NewParticipant(clusterID string, host string, port string) HelixParticipant
}
