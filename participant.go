package helix

// HelixParticipant is the process that performs the actual task in the distributed system.
type HelixParticipant interface {

	// Start will startup the participant.
	Start() error

	// Close will stop all jobs of the participant.
	Close()

	// RegisterStateModel associates state trasition functions with the participant.
	RegisterStateModel(name string, sm StateModel) error
}
