package helix

// HelixAdmin handles the administration task for the Helix cluster.
type HelixAdmin interface {

	// Connect will connect to the storage.
	Connect() error

	// Close will release resources.
	Close()

	// AddCluster add a managed cluster to Helix.
	AddCluster(cluster string) error

	// Clusters return all Helix managed clusters.
	Clusters() ([]string, error)

	// DropCluster removes a Helix managed cluster.
	DropCluster(cluster string) error

	// AllowParticipantAutoJoin permits a partitipant work without calling AddInstance beforehand.
	// By default this feature is off.
	AllowParticipantAutoJoin(cluster string, yes bool) error

	//
	SetConfig(cluster string, scope HelixConfigScope, properties map[string]string) error

	//
	GetConfig(cluster string, scope HelixConfigScope, keys []string) (map[string]interface{}, error)

	// Add an instance to a cluster.
	AddInstance(cluster string, config InstanceConfig) error

	// Drop an instance from a cluster.
	DropInstance(cluster string, ic InstanceConfig) error

	// Get a list of instances under a cluster.
	Instances(cluster string) ([]string, error)

	// Add a resource to a cluster.
	AddResource(cluster string, resource string, option AddResourceOption) error

	// Drop a resource from a cluster.
	DropResource(cluster string, resource string) error

	// EnableResource enables the specified resource in the cluster.
	EnableResource(cluster string, resource string) error

	// DisableResource disables the specified resource in the cluster.
	DisableResource(cluster string, resource string) error

	// AddStateModelDef adds a state model to a cluster.
	AddStateModelDef(cluster string, stateModel string, definition *Record) error

	// StateModelDefs gets a list of state model names under a cluster.
	StateModelDefs(cluster string) ([]string, error)

	// Rebalance a resource in cluster.
	Rebalance(cluster string, resource string, replica int) error

	// TODO
	//Resources(cluster string) ([]string, error)
	//setConstraint(String clusterName, ConstraintType constraintType, String constraintId, ConstraintItem constraintItem)
	// getInstanceConfig(String clusterName, String instanceName)
	// getResourceIdealState(String clusterName, String resourceName)
	// enablePartition(boolean enabled, String clusterName, String instanceName,
	//String resourceName, List<String> partitionNames)

}
