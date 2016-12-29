package helix

import (
	"github.com/funkygao/go-helix/model"
)

// HelixAdmin handles the administration task for the Helix cluster.
type HelixAdmin interface {

	// Connect will connect to the storage.
	Connect() error

	// Disconnect will disconnect from the storage and release related resources.
	Disconnect()

	// AddCluster add a managed cluster to Helix.
	AddCluster(cluster string) error

	// EnableCluster disable/enable a cluster.
	EnableCluster(cluster string, yes bool) error

	// AddClusterToGrandCluster adds a cluster and also adds this cluster as a resource group in the super cluster.
	AddClusterToGrandCluster(cluster, grandCluster string) error

	// Clusters return all Helix managed clusters under '/'.
	Clusters() ([]string, error)

	// DropCluster removes a Helix managed cluster.
	DropCluster(cluster string) error

	// IsClusterSetup checks if a cluster is setup ok.
	IsClusterSetup(cluster string) (bool, error)

	// AllowParticipantAutoJoin permits a partitipant work without calling AddInstance beforehand.
	// By default this feature is off.
	AllowParticipantAutoJoin(cluster string, yes bool) error

	// ControllerLeader returns the active controller leader of a cluster.
	ControllerLeader(cluster string) string

	// ControllerHistory returns all controller instance names in history.
	ControllerHistory(cluster string) ([]string, error)

	// Add a node to a cluster.
	// node has the form of host_port.
	AddNode(cluster string, node string) error

	// DropNode drops a node from a cluster.
	DropNode(cluster string, node string) error

	// Add an instance to a cluster.
	AddInstance(cluster string, ic *model.InstanceConfig) error

	// AddInstanceTag adds a tag to an instance.
	AddInstanceTag(cluster, instance, tag string) error

	// RemoveInstanceTag removes a tag from an instance.
	RemoveInstanceTag(cluster, instance, tag string) error

	// DropInstance drops an instance from a cluster.
	DropInstance(cluster string, ic *model.InstanceConfig) error

	// Instances returns a list of instances participating under a cluster.
	Instances(cluster string) ([]string, error)

	// LiveInstances returns a list of live instances participating under a cluster.
	LiveInstances(cluster string) ([]string, error)

	// InstancesWithTag returns a list of resources in a cluster with a tag.
	InstancesWithTag(cluster, tag string) ([]string, error)

	// InstanceConfig returns configuration information of an instance in a cluster.
	InstanceConfig(cluster, instance string) (*model.InstanceConfig, error)

	// Enable or disable an instance.
	EnableInstance(cluster, instance string, yes bool) error

	// ResourceIdealState returns ideal state for a resource.
	ResourceIdealState(cluster, resource string) (*model.IdealState, error)

	// SetResourceIdealState sets ideal state for a resource.
	SetResourceIdealState(cluster, resource string, is *model.IdealState) error

	// Add a resource to a cluster.
	AddResource(cluster string, resource string, option AddResourceOption) error

	// DropResource removes the specified resource from the cluster.
	DropResource(cluster string, resource string) error

	// Resources returns a list of resources managed by the helix cluster.
	Resources(cluster string) ([]string, error)

	// ResourcesWithTag returns a list of resources in a cluster with a tag.
	ResourcesWithTag(cluster, tag string) ([]string, error)

	// EnableResource enables/disables the specified resource in the cluster.
	EnableResource(cluster string, resource string, yes bool) error

	// ScaleResource hot change partition num of a resource.
	ScaleResource(cluster string, resource string, partitions int) error

	// EnablePartitions disable or enable a list of partitions on an instance.
	EnablePartitions(cluster, resource string, partitions []string, yes bool) error

	// AddStateModelDef adds a state model to a cluster.
	AddStateModelDef(cluster string, stateModel string, definition *model.StateModelDef) error

	// StateModelDefs gets a list of state model names under a cluster.
	StateModelDefs(cluster string) ([]string, error)

	// StateModelDef returns a state model definition in a cluster.
	StateModelDef(cluster, stateModel string) (*model.StateModelDef, error)

	// ResourceExternalView gets external view for a resource.
	ResourceExternalView(cluster string, resource string) (*model.ExternalView, error)

	// Rebalance a resource in cluster.
	Rebalance(cluster string, resource string, replica int) error

	// SetConfig set the configuration values for the cluster, defined by the config scope.
	// TODO
	SetConfig(cluster string, scope HelixConfigScope, properties map[string]string, ident ...string) error

	// DropConfig will drop properties of keys from a configuration in simple field.
	DropConfig(scope HelixConfigScope, keys []string, ident ...string) error

	// GetConfig obtains the configuration value of a property, defined by a config scope.
	// TODO
	GetConfig(cluster string, scope HelixConfigScope, keys []string, ident ...string) (map[string]interface{}, error)

	AddConstaint()
	RemoveConstaint()
	Constraints()

	// SetInstallPath will setup the local helix installation base path.
	// TODO kill this
	SetInstallPath(path string)
}
