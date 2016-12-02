package helix

type HelixService interface {

	//
	Start() error

	//
	Stop()

	//
	Started() bool
}
