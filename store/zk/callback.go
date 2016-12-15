package zk

type CallbackHandler struct {
	*Manager

	path     string
	listener interface{}
}

func (cb *CallbackHandler) Init() {

}
