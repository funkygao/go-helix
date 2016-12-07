package zk

import (
	"strconv"
	"strings"
	"sync"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
)

type Admin struct {
	sync.RWMutex
	closeOnce sync.Once

	zkSvr string
	*connection
	connected bool
}

// NewZkHelixAdmin creates a HelixAdmin implementation with zk as storage.
func NewZkHelixAdmin(zkSvr string, options ...zkConnOption) helix.HelixAdmin {
	admin := &Admin{
		zkSvr:      zkSvr,
		connected:  false,
		connection: newConnection(zkSvr),
	}
	for _, option := range options {
		option(admin.connection)
	}
	return admin
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

	if err := adm.connection.Connect(); err != nil {
		return err
	}

	adm.connected = true
	return nil
}

func (adm *Admin) Close() {
	adm.closeOnce.Do(func() {
		adm.Lock()
		if adm.connected {
			adm.Disconnect()
			adm.connected = false
		}
		adm.Unlock()
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

	clusterNode := model.NewRecord(cluster)
	adm.CreateRecordWithPath(kb.clusterConfig(), clusterNode)

	adm.CreateEmptyNode(kb.controller())
	adm.CreateEmptyNode(kb.controllerErrors())
	adm.CreateEmptyNode(kb.controllerHistory())
	adm.CreateEmptyNode(kb.controllerMessages())
	adm.CreateEmptyNode(kb.controllerStatusUpdates())

	ok, err := adm.IsClusterSetup(cluster)
	if err != nil {
		return err
	} else if !ok {
		return helix.ErrPartialSuccess
	}
	return nil
}

func (adm Admin) DropCluster(cluster string) error {
	kb := keyBuilder{clusterID: cluster}
	return adm.DeleteTree(kb.cluster())
}

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

func (adm Admin) ClusterInfo(cluster string) (instances []string, resources []string, err error) {
	if ok, e := adm.IsClusterSetup(cluster); !ok || e != nil {
		err = helix.ErrClusterNotSetup
		return
	}

	kb := keyBuilder{clusterID: cluster}
	resources, err = adm.Children(kb.idealStates())
	if err != nil {
		return
	}

	instances, err = adm.Children(kb.instances())
	return
}

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

func (adm Admin) AddInstance(cluster string, config model.InstanceConfig) error {
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
	n := model.NewRecord(node)
	n.SetSimpleField("HELIX_HOST", parts[0])
	n.SetSimpleField("HELIX_PORT", parts[1])
	// HELIX_ENABLED not set yet

	return any(
		adm.CreateRecordWithPath(path, n),
		adm.CreateEmptyNode(kb.instance(node)),
		adm.CreateEmptyNode(kb.messages(node)),
		adm.CreateEmptyNode(kb.currentStates(node)),
		adm.CreateEmptyNode(kb.errorsR(node)),
		adm.CreateEmptyNode(kb.statusUpdates(node)),
		adm.CreateEmptyNode(kb.healthReport(node)),
	)
}

func (adm Admin) DropInstance(cluster string, ic model.InstanceConfig) error {
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
	if err := adm.DeleteTree(kb.participantConfig(node)); err != nil {
		return err
	}

	// delete /<cluster>/INSTANCES/<node>
	return adm.DeleteTree(kb.instance(node))
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

	is := model.NewRecord(resource)
	is.SetSimpleField("NUM_PARTITIONS", strconv.Itoa(option.Partitions))
	is.SetSimpleField("REPLICAS", "0")
	is.SetSimpleField("REBALANCE_MODE", option.RebalancerMode)
	is.SetSimpleField("STATE_MODEL_DEF_REF", option.StateModel)
	is.SetSimpleField("STATE_MODEL_FACTORY_NAME", "DEFAULT")
	if option.MaxPartitionsPerInstance > 0 {
		is.SetIntField("MAX_PARTITIONS_PER_INSTANCE", option.MaxPartitionsPerInstance)
	}
	if option.BucketSize > 0 {
		is.SetSimpleField("BUCKET_SIZE", strconv.Itoa(option.BucketSize))
	}
	switch option.RebalancerMode {
	case helix.RebalancerModeFullAuto:
		// helix manages both state and location
	case helix.RebalancerModeSemiAuto:
		// helix manages state, app manages location constraint
		//is.ListFields
	case helix.RebalancerModeCustomized:
		// The application needs to implement a callback interface that Helix invokes when
		// the cluster state changes. Within this callback, the application can recompute
		// the idealstate. Helix will then issue appropriate transitions such that Idealstate
		// and Currentstate converges.
	case helix.RebalancerModeUserDefined:
	}

	return adm.CreateRecordWithPath(kb.idealStateForResource(resource), is)
}

func (adm Admin) DropResource(cluster string, resource string) error {
	// make sure the cluster is already setup
	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return helix.ErrClusterNotSetup
	}

	// make sure the path for the ideal state does not exit
	kb := keyBuilder{clusterID: cluster}
	if err := adm.DeleteTree(kb.idealStateForResource(resource)); err != nil {
		return err
	}
	return adm.DeleteTree(kb.resourceConfig(resource))
}

func (adm Admin) EnableResource(cluster string, resource string) error {
	return adm.setResourceEnabled(cluster, resource, true)
}

// DisableResource disables the specified resource in the cluster.
func (adm Admin) DisableResource(cluster string, resource string) error {
	return adm.setResourceEnabled(cluster, resource, false)
}

func (adm Admin) setResourceEnabled(cluster string, resource string, enabled bool) error {
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
	if enabled {
		return adm.UpdateSimpleField(isPath, "HELIX_ENABLED", "true")
	}
	return adm.UpdateSimpleField(isPath, "HELIX_ENABLED", "false")
}

func (adm Admin) Resources(cluster string) ([]string, error) {
	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return nil, helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	return adm.Children(kb.idealStates())
}

func (adm Admin) ResetResource(cluster string, resource string) error {
	return nil
}

func (adm Admin) InstanceInfo(cluster string, ic model.InstanceConfig) (*model.Record, error) {
	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return nil, helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	instanceCfg := kb.participantConfig(ic.Node())
	if exists, err := adm.Exists(instanceCfg); !exists || err != nil {
		if !exists {
			return nil, helix.ErrNodeNotExist
		}
		return nil, err
	}

	return adm.GetRecordFromPath(instanceCfg)
}

// Instances returns lists of instances.
func (adm Admin) Instances(cluster string) ([]string, error) {
	// make sure the cluster is already setup
	if ok, err := adm.IsClusterSetup(cluster); !ok || err != nil {
		return nil, helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	return adm.Children(kb.instances())
}

// TODO model.StateModelDef
func (adm Admin) AddStateModelDef(cluster string, stateModel string, definition *model.Record) error {
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
