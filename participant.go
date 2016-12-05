package helix

// HelixParticipant is the process that performs the actual task in the distributed system.
type HelixParticipant interface {
	HelixService

	// Manager returns the HelixManager.
	Manager() HelixManager

	// RegisterStateModel associates state trasition functions with the participant.
	RegisterStateModel(name string, sm *StateModel) error
}
