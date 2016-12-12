package helix

import (
	"github.com/funkygao/go-helix/model"
)

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

	// ClusterInfo returns the existing resources and instances in the cluster.
	ClusterInfo(cluster string) (instances []string, resources []string, err error)

	// DropCluster removes a Helix managed cluster.
	DropCluster(cluster string) error

	// AllowParticipantAutoJoin permits a partitipant work without calling AddInstance beforehand.
	// By default this feature is off.
	AllowParticipantAutoJoin(cluster string, yes bool) error

	// SetConfig set the configuration values for the cluster, defined by the config scope.
	SetConfig(cluster string, scope HelixConfigScope, properties map[string]string) error

	// GetConfig obtains the configuration value of a property, defined by a config scope.
	GetConfig(cluster string, scope HelixConfigScope, keys []string) (map[string]interface{}, error)

	// ControllerHistory returns all controller instance names in history.
	ControllerHistory(cluster string) ([]string, error)

	// Add a node to a cluster.
	// node has the form of host_port.
	AddNode(cluster string, node string) error

	// Add an instance to a cluster.
	AddInstance(cluster string, config model.InstanceConfig) error

	// Drop an instance from a cluster.
	DropInstance(cluster string, ic model.InstanceConfig) error

	// Get a list of instances participating under a cluster.
	Instances(cluster string) ([]string, error)

	InstanceInfo(cluster string, ic model.InstanceConfig) (*model.Record, error)

	// Enable or disable an instance.
	EnableInstance(cluster, instanceName string, yes bool) error

	//
	SetResourceIdealState(cluster, instanceName string, is *model.IdealState) error

	// Add a resource to a cluster.
	AddResource(cluster string, resource string, option AddResourceOption) error

	// DropResource removes the specified resource from the cluster.
	DropResource(cluster string, resource string) error

	// Resources shows a list of resources managed by the helix cluster.
	Resources(cluster string) ([]string, error)

	// EnableResource enables the specified resource in the cluster.
	EnableResource(cluster string, resource string) error

	// DisableResource disables the specified resource in the cluster.
	DisableResource(cluster string, resource string) error

	// ResetResource ERROR state
	ResetResource(cluster string, resource string) error

	// AddStateModelDef adds a state model to a cluster.
	AddStateModelDef(cluster string, stateModel string, definition *model.Record) error

	// StateModelDefs gets a list of state model names under a cluster.
	StateModelDefs(cluster string) ([]string, error)

	// Rebalance a resource in cluster.
	Rebalance(cluster string, resource string, replica int) error

	// SetInstallPath will setup the local helix installation base path.
	SetInstallPath(path string)

	// TODO
	//setConstraint(String clusterName, ConstraintType constraintType, String constraintId, ConstraintItem constraintItem)
	// getInstanceConfig(String clusterName, String instanceName)
	// getResourceIdealState(String clusterName, String resourceName)
	// enablePartition(boolean enabled, String clusterName, String instanceName,
	//String resourceName, List<String> partitionNames)

}
