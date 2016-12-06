package helix

// HelixManager is a common component that connects each system component with the controller.
type HelixManager interface {

	// Connect the participant to storage and start housekeeping.
	Connect() error

	// Close will disconnect the participant from storage and Helix controller.
	Close()

	StateMachineEngine() StateMachineEngine

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
}
