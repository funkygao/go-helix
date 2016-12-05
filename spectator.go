package helix

// HelixSpectator is a Helix role that does not participate the cluster state transition
// but only read cluster data, or listen to cluster updates.
type HelixSpectator interface {
	HelixService
	HelixManager
}
