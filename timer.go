package helix

//  HelixTimerTask is an interface for defining a generic task to run periodically.
type HelixTimerTask interface {
	Start() error

	Stop()
}
