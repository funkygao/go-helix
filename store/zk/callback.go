package zk

type CallbackHandler struct {
	*Manager

	path     string
	listener interface{}
}

func newCallbackHandler(mgr *Manager, listener interface{}) *CallbackHandler {
	return &CallbackHandler{
		Manager:  mgr,
		listener: listener,
	}
}

func (cb *CallbackHandler) Init() {

}

func (cb *CallbackHandler) Reset() {

}
