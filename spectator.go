package helix

// HelixSpectator is a Helix role that does not participate the cluster state transition
// but only read cluster data, or listen to cluster updates.
type HelixSpectator interface {

	//
	Start() error

	//
	Close()

	// AddExternalViewChangeListener add a listener to external view changes.
	AddExternalViewChangeListener(ExternalViewChangeListener)

	// AddLiveInstanceChangeListener add a listener to live instance changes.
	AddLiveInstanceChangeListener(LiveInstanceChangeListener)

	// AddCurrentStateChangeListener add a listener to current state changes of the specified instance.
	AddCurrentStateChangeListener(instance string, listener CurrentStateChangeListener)

	// AddMessageListener adds a listener to the messages of an instance.
	AddMessageListener(instance string, listener MessageListener)

	// AddControllerMessageListener add a listener to controller messages.
	AddControllerMessageListener(ControllerMessageListener)

	// AddIdealStateChangeListener add a listener to the cluster ideal state changes.
	AddIdealStateChangeListener(IdealStateChangeListener)

	// AddInstanceConfigChangeListener add a listener to instance config changes.
	AddInstanceConfigChangeListener(InstanceConfigChangeListener)

	// TODO
	GetControllerMessages() []*Record
	GetInstanceMessages(instance string) []*Record
	GetLiveInstances() []*Record
	GetExternalView() []*Record
}
