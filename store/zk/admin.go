package zk

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/funkygao/go-helix"
)

type Admin struct {
	sync.RWMutex

	zkSvr string
	*connection
	connected bool
	closeOnce sync.Once
}

func NewZKHelixAdmin(zkSvr string) helix.HelixAdmin {
	return &Admin{
		zkSvr:     zkSvr,
		connected: false,
	}
}

func (adm *Admin) Connect() error {
	adm.RLock()
	if adm.connected {
		adm.RUnlock()
		return nil
	}
	adm.RUnlock()

	adm.Lock()
	defer adm.Unlock()
	if adm.connected {
		return nil
	}

	adm.connection = newConnection(adm.zkSvr)
	if err := adm.connection.Connect(); err != nil {
		return err
	}

	adm.connected = true
	return nil
}

func (adm *Admin) Close() {
	adm.closeOnce.Do(func() {
		if adm.connected {
			adm.Disconnect()
			adm.connected = false
		}
	})
}

func (adm Admin) AddCluster(cluster string) error {
	kb := keyBuilder{clusterID: cluster}

	// avoid dup cluster
	exists, err := adm.Exists(kb.cluster())
	if err != nil {
		return err
	}
	if exists {
		return helix.ErrNodeAlreadyExists
	}

	adm.CreateEmptyNode(kb.cluster())
	adm.CreateEmptyNode(kb.propertyStore())
	adm.CreateEmptyNode(kb.instances())
	adm.CreateEmptyNode(kb.idealStates())
	adm.CreateEmptyNode(kb.externalView())
	adm.CreateEmptyNode(kb.liveInstances())

	adm.CreateEmptyNode(kb.stateModelDefs())
	adm.CreateRecordWithData(kb.stateModelDef(helix.StateModelLeaderStandby), helix.HelixDefaultStateModels[helix.StateModelLeaderStandby])
	adm.CreateRecordWithData(kb.stateModelDef(helix.StateModelMasterSlave), helix.HelixDefaultStateModels[helix.StateModelMasterSlave])
	adm.CreateRecordWithData(kb.stateModelDef(helix.StateModelOnlineOffline), helix.HelixDefaultStateModels[helix.StateModelOnlineOffline])
	adm.CreateRecordWithData(kb.stateModelDef(helix.StateModelDefaultSchemata), helix.HelixDefaultStateModels[helix.StateModelDefaultSchemata])
	adm.CreateRecordWithData(kb.stateModelDef(helix.StateModelSchedulerTaskQueue), helix.HelixDefaultStateModels[helix.StateModelSchedulerTaskQueue])
	adm.CreateRecordWithData(kb.stateModelDef(helix.StateModelTask), helix.HelixDefaultStateModels[helix.StateModelTask])

	adm.CreateEmptyNode(kb.configs())
	adm.CreateEmptyNode(kb.participantConfigs())
	adm.CreateEmptyNode(kb.resourceConfigs())
	adm.CreateEmptyNode(kb.clusterConfigs())

	clusterNode := helix.NewRecord(cluster)
	adm.CreateRecordWithPath(kb.clusterConfig(), clusterNode)

	adm.CreateEmptyNode(kb.controller())
	adm.CreateEmptyNode(kb.controllerErrors())
	adm.CreateEmptyNode(kb.controllerHistory())
	adm.CreateEmptyNode(kb.controllerMessages())
	adm.CreateEmptyNode(kb.controllerStatusUpdates())

	return nil
}

// Clusters shows all Helix managed clusters in the admected zookeeper cluster
func (adm Admin) Clusters() ([]string, error) {
	children, err := adm.Children("/")
	if err != nil {
		return nil, err
	}

	var clusters []string
	for _, cluster := range children {
		if ok, err := adm.IsClusterSetup(cluster); ok && err == nil {
			clusters = append(clusters, cluster)
		}
	}

	return clusters, nil
}

// DropCluster removes a helix cluster from zookeeper. This will remove the
// znode named after the cluster name from the zookeeper root.
func (adm Admin) DropCluster(cluster string) error {
	kb := keyBuilder{clusterID: cluster}
	return adm.DeleteTree(kb.cluster())
}

// ListClusterInfo shows the existing resources and instances in the cluster
func (adm Admin) ListClusterInfo(cluster string) (string, error) {
	// make sure the cluster is already setup
	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return "", helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	resources, err := adm.Children(kb.idealStates())
	if err != nil {
		return "", err
	}

	instances, err := adm.Children(kb.instances())
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

// SetConfig set the configuration values for the cluster, defined by the config scope
func (adm Admin) SetConfig(cluster string, scope helix.HelixConfigScope, properties map[string]string) error {
	switch scope {
	case helix.ConfigScopeCluster:
		if allow, ok := properties["allowParticipantAutoJoin"]; ok {
			kb := keyBuilder{clusterID: cluster}
			if strings.ToLower(allow) == "true" {
				// false by default
				adm.UpdateSimpleField(kb.clusterConfig(), "allowParticipantAutoJoin", "true")
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
func (adm Admin) GetConfig(cluster string, scope helix.HelixConfigScope, keys []string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	switch scope {
	case helix.ConfigScopeCluster:
		kb := keyBuilder{clusterID: cluster}
		for _, k := range keys {
			result[k] = adm.GetSimpleFieldValueByKey(kb.clusterConfig(), k)
		}

	case helix.ConfigScopeConstraint:
	case helix.ConfigScopeParticipant:
	case helix.ConfigScopePartition:
	case helix.ConfigScopeResource:
	}

	return result, nil
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

func (adm Admin) AddInstance(cluster string, config helix.InstanceConfig) error {
	return adm.AddNode(cluster, config.Node())
}

// AddNode is the internal implementation corresponding to command
// ./helix-admin.sh --zkSvr <ZookeeperServerAddress> --addNode <clusterName instanceId>
// node is in the form of host_port
func (adm Admin) AddNode(cluster string, node string) error {
	if ok, err := adm.IsClusterSetup(cluster); ok == false || err != nil {
		return helix.ErrClusterNotSetup
	}

	// check if node already exists under /<cluster>/CONFIGS/PARTICIPANT/<NODE>
	kb := keyBuilder{clusterID: cluster}
	path := kb.participantConfig(node)
	exists, err := adm.Exists(path)
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

	adm.CreateRecordWithPath(path, n)
	adm.CreateEmptyNode(kb.instance(node))
	adm.CreateEmptyNode(kb.messages(node))
	adm.CreateEmptyNode(kb.currentStates(node))
	adm.CreateEmptyNode(kb.errorsR(node))
	adm.CreateEmptyNode(kb.statusUpdates(node))

	return nil
}

func (adm Admin) DropInstance(cluster string, ic helix.InstanceConfig) error {
	return adm.DropNode(cluster, ic.Node())
}

// DropNode removes a node from a cluster. The corresponding znodes
// in zookeeper will be removed.
func (adm Admin) DropNode(cluster string, node string) error {
	// check if node already exists under /<cluster>/CONFIGS/PARTICIPANT/<node>
	kb := keyBuilder{clusterID: cluster}
	if exists, err := adm.Exists(kb.participantConfig(node)); !exists || err != nil {
		return helix.ErrNodeNotExist
	}

	// check if node exist under instance: /<cluster>/INSTANCES/<node>
	if exists, err := adm.Exists(kb.instance(node)); !exists || err != nil {
		return helix.ErrInstanceNotExist
	}

	// delete /<cluster>/CONFIGS/PARTICIPANT/<node>
	adm.DeleteTree(kb.participantConfig(node))

	// delete /<cluster>/INSTANCES/<node>
	adm.DeleteTree(kb.instance(node))

	return nil
}

func (adm Admin) AddResource(cluster string, resource string, option helix.AddResourceOption) error {
	if err := option.Validate(); err != nil {
		return err
	}

	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}

	// make sure the state model def exists
	if exists, err := adm.Exists(kb.stateModelDef(option.StateModel)); !exists || err != nil {
		return helix.ErrStateModelDefNotExist
	}

	// make sure the path for the ideal state does not exit
	if exists, err := adm.Exists(kb.idealStateForResource(resource)); exists || err != nil {
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

	return adm.CreateRecordWithPath(kb.idealStateForResource(resource), is)
}

// DropResource removes the specified resource from the cluster.
func (adm Admin) DropResource(cluster string, resource string) error {
	// make sure the cluster is already setup
	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	// make sure the path for the ideal state does not exit
	kb := keyBuilder{clusterID: cluster}
	adm.DeleteTree(kb.idealStateForResource(resource))
	adm.DeleteTree(kb.resourceConfig(resource))

	return nil
}

// EnableResource enables the specified resource in the cluster.
func (adm Admin) EnableResource(cluster string, resource string) error {
	// make sure the cluster is already setup
	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	isPath := kb.idealStateForResource(resource)
	if exists, err := adm.Exists(isPath); !exists || err != nil {
		if !exists {
			return helix.ErrResourceNotExists
		}
		return err
	}

	// TODO: set the value at leaf node instead of the record level
	return adm.UpdateSimpleField(isPath, "HELIX_ENABLED", "true")
}

// DisableResource disables the specified resource in the cluster.
func (adm Admin) DisableResource(cluster string, resource string) error {
	// make sure the cluster is already setup
	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	isPath := kb.idealStateForResource(resource)
	if exists, err := adm.Exists(isPath); !exists || err != nil {
		if !exists {
			return helix.ErrResourceNotExists
		}

		return err
	}

	return adm.UpdateSimpleField(isPath, "HELIX_ENABLED", "false")
}

// ListResources shows a list of resources managed by the helix cluster
func (adm Admin) ListResources(cluster string) (string, error) {
	// make sure the cluster is already setup
	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return "", helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	resources, err := adm.Children(kb.idealStates())
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
	// make sure the cluster is already setup
	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return "", helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	instances, err := adm.Children(kb.instances())
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
	if !adm.connected {
		return "", helix.ErrNotConnected
	}

	// make sure the cluster is already setup
	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return "", helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	instanceCfg := kb.participantConfig(instance)
	if exists, err := adm.Exists(instanceCfg); !exists || err != nil {
		if !exists {
			return "", helix.ErrNodeNotExist
		}
		return "", err
	}

	r, err := adm.GetRecordFromPath(instanceCfg)
	if err != nil {
		return "", err
	}
	return r.String(), nil
}

// Instances returns lists of instances.
func (adm Admin) Instances(cluster string) ([]string, error) {
	if !adm.connected {
		return nil, helix.ErrNotConnected
	}

	kb := keyBuilder{clusterID: cluster}
	return adm.Children(kb.instances())
}

func (adm Admin) AddStateModelDef(cluster string, stateModel string, definition *helix.Record) error {
	kb := keyBuilder{clusterID: cluster}
	return adm.CreateRecordWithPath(kb.stateModelDef(stateModel), definition)
}

func (adm Admin) StateModelDefs(cluster string) ([]string, error) {
	kb := keyBuilder{clusterID: cluster}
	return adm.Children(kb.stateModelDefs())
}

// Rebalance not implemented yet TODO
func (adm Admin) Rebalance(cluster string, resource string, replica int) error {
	if !adm.connected {
		return helix.ErrNotConnected
	}

	return helix.ErrNotImplemented
}
