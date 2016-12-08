package helix

// HelixManager is a common component that connects each system component with the controller.
type HelixManager interface {

	// Connect will connect manager to storage and start housekeeping.
	Connect() error

	// Disconnect will disconnect manager from storage.
	Disconnect()

	// Cluster returns the cluster name associated with this cluster manager.
	Cluster() string

	// Instance returns the instance name used to connect to the cluster.
	Instance() string

	// InstanceType returns the manager instance type.
	InstanceType() InstanceType

	// SessionID returns the session id associated with the connection to cluster data store.
	SessionID() string

	// AddPreConnectCallback adds a callback that is invoked before a participant joins the cluster.
	AddPreConnectCallback(PreConnectCallback)

	// StateMachineEngine returns the sme of the participant.
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

	// HelixDataAccessor returns the client to perform read/write operations on the cluster data.
	HelixDataAccessor() HelixDataAccessor

	// MessagingService returns ClusterMessagingService which can be used to send cluster wide messages.
	MessagingService() ClusterMessagingService

	// ClusterManagementTool provides admin interface to setup and modify cluster.
	ClusterManagementTool() HelixAdmin
}
