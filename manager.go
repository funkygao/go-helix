package helix

// HelixManager is a common component that connects each system component with the controller.
type HelixManager interface {
	NewSpectator(clusterID string) HelixSpectator

	NewParticipant(clusterID string, host string, port string) HelixParticipant
}
