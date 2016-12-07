package helix

type HelixTimerTask interface {
	Start() error

	Stop()
}
