package helix

type HelixParticipant interface {

	// Connect the participant to storage and start housekeeping.
	Connect() error

	// Close will disconnect the participant from storage and Helix controller.
	Close()

	// RegisterStateModel associates state trasition functions with the participant.
	RegisterStateModel(name string, sm StateModel)
}
