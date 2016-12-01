package zk

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/funkygao/go-helix"
)

// Admin handles the administration task for the Helix cluster. Many of the operations
// are mirroring the implementions documented at
// http://helix.apache.org/0.7.0-incubating-docs/Quickstart.html

var _ helix.HelixAdmin = &Admin{}

type Admin struct {
	zkSvr string

	conn      *connection
	connected bool
}

func NewZKHelixAdmin(zkSvr string) *Admin {
	return &Admin{zkSvr: zkSvr}
}

func (adm *Admin) Connect() error {
	adm.conn = newConnection(adm.zkSvr)
	if err := adm.conn.Connect(); err != nil {
		return err
	}

	adm.connected = true
	return nil
}

func (adm *Admin) Disconnect() {
	if adm.connected {
		adm.conn.Disconnect()
		adm.connected = false
	}
}

// AddCluster add a cluster to Helix. As a result, a znode will be created in zookeeper
// root named after the cluster name, and corresponding data structures are populated
// under this znode.
func (adm Admin) AddCluster(cluster string) error {
	conn := newConnection(adm.zkSvr)
	if err := conn.Connect(); err != nil {
		return err
	}
	defer conn.Disconnect()

	kb := keyBuilder{clusterID: cluster}

	// avoid dup cluster
	exists, err := conn.Exists(kb.cluster())
	if err != nil {
		return err
	}
	if exists {
		return helix.ErrNodeAlreadyExists
	}

	conn.CreateEmptyNode(kb.cluster())
	conn.CreateEmptyNode(kb.propertyStore())
	conn.CreateEmptyNode(kb.instances())
	conn.CreateEmptyNode(kb.idealStates())
	conn.CreateEmptyNode(kb.externalView())
	conn.CreateEmptyNode(kb.liveInstances())

	conn.CreateEmptyNode(kb.stateModels())
	conn.CreateRecordWithData(kb.stateModel(helix.StateModelLeaderStandby), helix.HelixDefaultNodes[helix.StateModelLeaderStandby])
	conn.CreateRecordWithData(kb.stateModel(helix.StateModelMasterSlave), helix.HelixDefaultNodes[helix.StateModelMasterSlave])
	conn.CreateRecordWithData(kb.stateModel(helix.StateModelOnlineOffline), helix.HelixDefaultNodes[helix.StateModelOnlineOffline])
	conn.CreateRecordWithData(kb.stateModel("STORAGE_DEFAULT_SM_SCHEMATA"), helix.HelixDefaultNodes["STORAGE_DEFAULT_SM_SCHEMATA"])
	conn.CreateRecordWithData(kb.stateModel(helix.StateModelSchedulerTaskQueue), helix.HelixDefaultNodes[helix.StateModelSchedulerTaskQueue])
	conn.CreateRecordWithData(kb.stateModel(helix.StateModelTask), helix.HelixDefaultNodes[helix.StateModelTask])

	conn.CreateEmptyNode(kb.configs())
	conn.CreateEmptyNode(kb.participantConfigs())
	conn.CreateEmptyNode(kb.resourceConfigs())
	conn.CreateEmptyNode(kb.clusterConfigs())

	clusterNode := helix.NewRecord(cluster)
	conn.CreateRecordWithPath(kb.clusterConfig(), clusterNode)

	conn.CreateEmptyNode(kb.controller())
	conn.CreateEmptyNode(kb.controllerErrors())
	conn.CreateEmptyNode(kb.controllerHistory())
	conn.CreateEmptyNode(kb.controllerMessages())
	conn.CreateEmptyNode(kb.controllerStatusUpdates())

	return nil
}

// DropCluster removes a helix cluster from zookeeper. This will remove the
// znode named after the cluster name from the zookeeper root.
func (adm Admin) DropCluster(cluster string) error {
	conn := newConnection(adm.zkSvr)
	if err := conn.Connect(); err != nil {
		return err
	}
	defer conn.Disconnect()

	kb := keyBuilder{clusterID: cluster}
	return conn.DeleteTree(kb.cluster())
}

func (adm Admin) AllowParticipantAutoJoin(cluster string, yes bool) error {
	var properties = map[string]string{
		"allowParticipantAutoJoin": "false",
	}
	if yes {
		properties["allowParticipantAutoJoin"] = "true"
	}
	return adm.SetConfig(cluster, "CLUSTER", properties)
}

// ListClusterInfo shows the existing resources and instances in the cluster
func (adm Admin) ListClusterInfo(cluster string) (string, error) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return "", err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return "", helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	resources, err := conn.Children(kb.idealStates())
	if err != nil {
		return "", err
	}

	instances, err := conn.Children(kb.instances())
	if err != nil {
		return "", err
	}

	var buffer bytes.Buffer
	buffer.WriteString("Existing resources in cluster " + cluster + ":\n")

	for _, r := range resources {
		buffer.WriteString("  " + r + "\n")
	}

	buffer.WriteString("\nInstances in cluster " + cluster + ":\n")
	for _, i := range instances {
		buffer.WriteString("  " + i + "\n")
	}
	return buffer.String(), nil
}

// ListClusters shows all Helix managed clusters in the connected zookeeper cluster
func (adm Admin) ListClusters() ([]string, error) {
	conn := newConnection(adm.zkSvr)
	if err := conn.Connect(); err != nil {
		return nil, err
	}
	defer conn.Disconnect()

	children, err := conn.Children("/")
	if err != nil {
		return nil, err
	}

	var clusters []string
	for _, cluster := range children {
		if ok, err := conn.IsClusterSetup(cluster); ok && err == nil {
			clusters = append(clusters, cluster)
		}
	}

	return clusters, nil
}

// SetConfig set the configuration values for the cluster, defined by the config scope
func (adm Admin) SetConfig(cluster string, scope helix.HelixConfigScope, properties map[string]string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	switch scope {
	case helix.ConfigScopeCluster:
		if allow, ok := properties["allowParticipantAutoJoin"]; ok {
			kb := keyBuilder{clusterID: cluster}
			if strings.ToLower(allow) == "true" {
				// false by default
				conn.UpdateSimpleField(kb.clusterConfig(), "allowParticipantAutoJoin", "true")
			}
		}
	case helix.ConfigScopeConstraint:
	case helix.ConfigScopeParticipant:
	case helix.ConfigScopePartition:
	case helix.ConfigScopeResource:
	}

	return nil
}

// GetConfig obtains the configuration value of a property, defined by a config scope
func (adm Admin) GetConfig(cluster string, scope helix.HelixConfigScope, keys []string) map[string]interface{} {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return nil
	}
	defer conn.Disconnect()

	result := make(map[string]interface{})

	switch scope {
	case helix.ConfigScopeCluster:
		kb := keyBuilder{clusterID: cluster}
		for _, k := range keys {
			result[k] = conn.GetSimpleFieldValueByKey(kb.clusterConfig(), k)
		}

	case helix.ConfigScopeConstraint:
	case helix.ConfigScopeParticipant:
	case helix.ConfigScopePartition:
	case helix.ConfigScopeResource:
	}

	return result
}

func (adm Admin) AddInstance(cluster string, config helix.InstanceConfig) error {
	return adm.AddNode(cluster, config.Node())
}

// AddNode is the internal implementation corresponding to command
// ./helix-admin.sh --zkSvr <ZookeeperServerAddress> --addNode <clusterName instanceId>
// node is in the form of host_port
func (adm Admin) AddNode(cluster string, node string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	if ok, err := conn.IsClusterSetup(cluster); ok == false || err != nil {
		return helix.ErrClusterNotSetup
	}

	// check if node already exists under /<cluster>/CONFIGS/PARTICIPANT/<NODE>
	kb := keyBuilder{clusterID: cluster}
	path := kb.participantConfig(node)
	exists, err := conn.Exists(path)
	if err != nil {
		return err
	}
	if exists {
		return helix.ErrNodeAlreadyExists
	}

	// create new node for the participant
	parts := strings.Split(node, "_")
	n := helix.NewRecord(node)
	n.SetSimpleField("HELIX_HOST", parts[0])
	n.SetSimpleField("HELIX_PORT", parts[1])

	conn.CreateRecordWithPath(path, n)
	conn.CreateEmptyNode(kb.instance(node))
	conn.CreateEmptyNode(kb.messages(node))
	conn.CreateEmptyNode(kb.currentStates(node))
	conn.CreateEmptyNode(kb.errorsR(node))
	conn.CreateEmptyNode(kb.statusUpdates(node))

	return nil
}

func (adm Admin) DropInstance(cluster string, ic helix.InstanceConfig) error {
	return adm.DropNode(cluster, ic.Node())
}

// DropNode removes a node from a cluster. The corresponding znodes
// in zookeeper will be removed.
func (adm Admin) DropNode(cluster string, node string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	// check if node already exists under /<cluster>/CONFIGS/PARTICIPANT/<node>
	kb := keyBuilder{clusterID: cluster}
	if exists, err := conn.Exists(kb.participantConfig(node)); !exists || err != nil {
		return helix.ErrNodeNotExist
	}

	// check if node exist under instance: /<cluster>/INSTANCES/<node>
	if exists, err := conn.Exists(kb.instance(node)); !exists || err != nil {
		return helix.ErrInstanceNotExist
	}

	// delete /<cluster>/CONFIGS/PARTICIPANT/<node>
	conn.DeleteTree(kb.participantConfig(node))

	// delete /<cluster>/INSTANCES/<node>
	conn.DeleteTree(kb.instance(node))

	return nil
}

func (adm Admin) AddResourceWithOption(cluster string, resource string, option helix.AddResourceOption) error {
	if err := option.Validate(); err != nil {
		return err
	}

	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}

	// make sure the state model def exists
	if exists, err := conn.Exists(kb.stateModel(option.StateModel)); !exists || err != nil {
		return helix.ErrStateModelDefNotExist
	}

	// make sure the path for the ideal state does not exit
	if exists, err := conn.Exists(kb.idealStateForResource(resource)); exists || err != nil {
		if exists {
			return helix.ErrResourceExists
		}
		return err
	}

	is := helix.NewRecord(resource)
	is.SetSimpleField("NUM_PARTITIONS", strconv.Itoa(option.Partitions))
	is.SetSimpleField("REPLICAS", "0")
	is.SetSimpleField("REBALANCE_MODE", option.RebalancerMode) // TODO
	is.SetSimpleField("STATE_MODEL_DEF_REF", option.StateModel)
	is.SetSimpleField("STATE_MODEL_FACTORY_NAME", "DEFAULT")
	if option.MaxPartitionsPerInstance > 0 {
		is.SetIntField("MAX_PARTITIONS_PER_INSTANCE", option.MaxPartitionsPerInstance)
	}
	if option.BucketSize > 0 {
		is.SetSimpleField("BUCKET_SIZE", strconv.Itoa(option.BucketSize))
	}

	return conn.CreateRecordWithPath(kb.idealStateForResource(resource), is)
}

// AddResource implements the helix-admin.sh --addResource
// # helix-admin.sh --zkSvr <zk_address> --addResource <clustername> <resourceName> <numPartitions> <StateModelName>
// ./helix-admin.sh --zkSvr localhost:2199 --addResource MYCLUSTER myDB 6 MasterSlave
func (adm Admin) AddResource(cluster string, resource string, partitions int, stateModel string) error {
	option := helix.DefaultAddResourceOption(partitions, stateModel)
	return adm.AddResourceWithOption(cluster, resource, option)
}

// DropResource removes the specified resource from the cluster.
func (adm Admin) DropResource(cluster string, resource string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	// make sure the path for the ideal state does not exit
	kb := keyBuilder{clusterID: cluster}
	conn.DeleteTree(kb.idealStateForResource(resource))
	conn.DeleteTree(kb.resourceConfig(resource))

	return nil
}

// EnableResource enables the specified resource in the cluster
func (adm Admin) EnableResource(cluster string, resource string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	isPath := kb.idealStateForResource(resource)
	if exists, err := conn.Exists(isPath); !exists || err != nil {
		if !exists {
			return helix.ErrResourceNotExists
		}
		return err
	}

	// TODO: set the value at leaf node instead of the record level
	return conn.UpdateSimpleField(isPath, "HELIX_ENABLED", "true")
}

// DisableResource disables the specified resource in the cluster.
func (adm Admin) DisableResource(cluster string, resource string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	isPath := kb.idealStateForResource(resource)
	if exists, err := conn.Exists(isPath); !exists || err != nil {
		if !exists {
			return helix.ErrResourceNotExists
		}

		return err
	}

	return conn.UpdateSimpleField(isPath, "HELIX_ENABLED", "false")
}

// ListResources shows a list of resources managed by the helix cluster
func (adm Admin) ListResources(cluster string) (string, error) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return "", err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return "", helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	resources, err := conn.Children(kb.idealStates())
	if err != nil {
		return "", err
	}

	var buffer bytes.Buffer
	buffer.WriteString("Existing resources in cluster " + cluster + ":\n")

	for _, r := range resources {
		buffer.WriteString("  " + r + "\n")
	}

	return buffer.String(), nil
}

// ListInstances shows a list of instances participating the cluster.
func (adm Admin) ListInstances(cluster string) (string, error) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return "", err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return "", helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	instances, err := conn.Children(kb.instances())
	if err != nil {
		return "", err
	}

	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Existing instances in cluster %s:\n", cluster))

	for _, r := range instances {
		buffer.WriteString("  " + r + "\n")
	}

	return buffer.String(), nil
}

// ListInstanceInfo shows detailed information of an inspace in the helix cluster
func (adm Admin) ListInstanceInfo(cluster string, instance string) (string, error) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return "", err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return "", helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	instanceCfg := kb.participantConfig(instance)
	if exists, err := conn.Exists(instanceCfg); !exists || err != nil {
		if !exists {
			return "", helix.ErrNodeNotExist
		}
		return "", err
	}

	r, err := conn.GetRecordFromPath(instanceCfg)
	if err != nil {
		return "", err
	}
	return r.String(), nil
}

// GetInstances returns lists of instances
func (adm Admin) GetInstances(cluster string) ([]string, error) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return nil, err
	}
	defer conn.Disconnect()

	kb := keyBuilder{clusterID: cluster}
	return conn.Children(kb.instances())
}

// Rebalance not implemented yet TODO
func (adm Admin) Rebalance(cluster string, resource string, replica int) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		fmt.Println("Failed to connect to zookeeper.")
		return
	}
	defer conn.Disconnect()

	fmt.Println("Not implemented")
}
