package helix

type HelixParticipant interface {

	// Start will startup the participant.
	Start() error

	//
	Close()

	// RegisterStateModel associates state trasition functions with the participant.
	RegisterStateModel(name string, sm StateModel)
}
