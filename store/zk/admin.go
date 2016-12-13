package zk

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/zkclient"
)

type Admin struct {
	sync.RWMutex
	closeOnce sync.Once

	*connection

	// TODO kill this
	helixInstallPath string
}

// NewZkHelixAdmin creates a HelixAdmin implementation with zk as storage.
func NewZkHelixAdmin(zkSvr string, options ...zkclient.Option) helix.HelixAdmin {
	admin := newZkHelixAdminWithConn(newConnection(zkSvr))

	// apply additional options over the default
	for _, option := range options {
		option(admin.connection.Client)
	}

	return admin
}

func newZkHelixAdminWithConn(c *connection) *Admin {
	return &Admin{
		connection:       c,
		helixInstallPath: "/opt/helix",
	}
}

func (adm *Admin) Connect() error {
	adm.RLock()
	if adm.connected() {
		adm.RUnlock()
		return nil
	}
	adm.RUnlock()

	adm.Lock()
	defer adm.Unlock()
	if adm.connected() {
		return nil
	}

	if err := adm.connection.Connect(); err != nil {
		return err
	}

	return adm.connection.WaitUntilConnected(0)
}

func (adm *Admin) Disconnect() {
	adm.closeOnce.Do(func() {
		adm.Lock()
		if adm.connected() {
			adm.Disconnect()
		}
		adm.Unlock()
	})
}

func (adm *Admin) connected() bool {
	return adm.connection.IsConnected()
}

func (adm *Admin) SetInstallPath(path string) {
	adm.helixInstallPath = path
}

func (adm Admin) ControllerHistory(cluster string) ([]string, error) {
	kb := keyBuilder{clusterID: cluster}
	record, err := adm.GetRecord(kb.controllerHistory())
	if err != nil {
		return nil, err
	}

	return record.GetListField(cluster), nil
}

func (adm *Admin) AddCluster(cluster string) error {
	adm.Lock()
	defer adm.Unlock()

	kb := keyBuilder{clusterID: cluster}

	// avoid dup cluster
	exists, err := adm.Exists(kb.cluster())
	if err != nil {
		return err
	}
	if exists {
		return helix.ErrNodeAlreadyExists
	}

	adm.CreateEmptyPersistent(kb.cluster())
	adm.CreateEmptyPersistent(kb.propertyStore())
	adm.CreateEmptyPersistent(kb.instances())
	adm.CreateEmptyPersistent(kb.idealStates())
	adm.CreateEmptyPersistent(kb.externalView())
	adm.CreateEmptyPersistent(kb.liveInstances())

	// create all the default state mode definitions
	adm.CreateEmptyPersistent(kb.stateModelDefs())
	adm.CreatePersistent(kb.stateModelDef(helix.StateModelLeaderStandby), helix.HelixDefaultStateModels[helix.StateModelLeaderStandby])
	adm.CreatePersistent(kb.stateModelDef(helix.StateModelMasterSlave), helix.HelixDefaultStateModels[helix.StateModelMasterSlave])
	adm.CreatePersistent(kb.stateModelDef(helix.StateModelOnlineOffline), helix.HelixDefaultStateModels[helix.StateModelOnlineOffline])
	adm.CreatePersistent(kb.stateModelDef(helix.StateModelDefaultSchemata), helix.HelixDefaultStateModels[helix.StateModelDefaultSchemata])
	adm.CreatePersistent(kb.stateModelDef(helix.StateModelSchedulerTaskQueue), helix.HelixDefaultStateModels[helix.StateModelSchedulerTaskQueue])
	adm.CreatePersistent(kb.stateModelDef(helix.StateModelTask), helix.HelixDefaultStateModels[helix.StateModelTask])

	adm.CreateEmptyPersistent(kb.configs())
	adm.CreateEmptyPersistent(kb.participantConfigs())
	adm.CreateEmptyPersistent(kb.resourceConfigs())
	adm.CreateEmptyPersistent(kb.clusterConfigs())

	clusterNode := model.NewRecord(cluster)
	adm.CreatePersistentRecord(kb.clusterConfig(), clusterNode)

	adm.CreateEmptyPersistent(kb.controller())
	adm.CreateEmptyPersistent(kb.controllerErrors())
	adm.CreateEmptyPersistent(kb.controllerHistory())
	adm.CreateEmptyPersistent(kb.controllerMessages())
	adm.CreateEmptyPersistent(kb.controllerStatusUpdates())

	// checkup before we return
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

	// cannot drop cluster if there is live instances running
	liveInstances, err := adm.Children(kb.liveInstances())
	if err != nil {
		return err
	}
	if len(liveInstances) > 0 {
		return helix.ErrNotEmpty
	}

	// cannot dro cluster if there is controller running
	leader, err := adm.Get(kb.controllerLeader())
	if err == nil && len(leader) > 0 {
		return helix.ErrNotEmpty
	}

	return adm.DeleteTree(kb.cluster())
}

func (adm Admin) EnableCluster(cluster string, yes bool) error {
	kb := keyBuilder{clusterID: cluster}
	if yes {
		return adm.Delete(kb.pause())
	} else {
		return adm.CreatePersistent(kb.pause(), []byte("pause"))
	}
}

func (adm Admin) AddClusterToGrandCluster(cluster, grandCluster string) (err error) {
	if ok, e := adm.IsClusterSetup(cluster); !ok || e != nil {
		err = helix.ErrClusterNotSetup
		return
	}
	if ok, e := adm.IsClusterSetup(grandCluster); !ok || e != nil {
		err = helix.ErrClusterNotSetup
		return
	}

	// TODO
	return helix.ErrNotImplemented
}

func (adm Admin) Clusters() ([]string, error) {
	children, err := adm.Children("/")
	if err != nil {
		return nil, err
	}

	var clusters []string
	for _, cluster := range children {
		if ok, err := adm.IsClusterSetup(cluster); err == nil && ok {
			clusters = append(clusters, cluster)
		}
	}

	return clusters, nil
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

func (adm Admin) DropConfig(scope helix.HelixConfigScope, keys []string) error {
	return helix.ErrNotImplemented
}

func (adm Admin) GetConfig(cluster string, scope helix.HelixConfigScope, keys []string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	switch scope {
	case helix.ConfigScopeCluster:
		kb := keyBuilder{clusterID: cluster}
		for _, k := range keys {
			result[k], _ = adm.GetSimpleFieldValueByKey(kb.clusterConfig(), k)
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

func (adm Admin) AddInstanceTag(cluster, instance, tag string) error {
	if ok, err := adm.IsClusterSetup(cluster); ok == false || err != nil {
		return helix.ErrClusterNotSetup
	}
	if ok, err := adm.IsInstanceSetup(cluster, instance); ok == false || err != nil {
		return helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	data, err := adm.Get(kb.participantConfig(instance))
	if err != nil {
		return err
	}

	r, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}
	ic := model.NewInstanceConfigFromRecord(r)
	ic.AddTag(tag)
	return adm.Set(kb.participantConfig(instance), ic.Marshal())
}

func (adm Admin) RemoveInstanceTag(cluster, instance, tag string) error {
	if ok, err := adm.IsClusterSetup(cluster); ok == false || err != nil {
		return helix.ErrClusterNotSetup
	}
	if ok, err := adm.IsInstanceSetup(cluster, instance); ok == false || err != nil {
		return helix.ErrClusterNotSetup
	}

	kb := keyBuilder{clusterID: cluster}
	data, err := adm.Get(kb.participantConfig(instance))
	if err != nil {
		return err
	}

	r, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}
	ic := model.NewInstanceConfigFromRecord(r)
	ic.RemoveTag(tag)
	return adm.Set(kb.participantConfig(instance), ic.Marshal())
}

func (adm Admin) InstanceConfig(cluster, instance string) (*model.InstanceConfig, error) {
	return nil, helix.ErrNotImplemented
}

func (adm Admin) InstancesWithTag(cluster, tag string) ([]string, error) {
	return nil, helix.ErrNotImplemented
}

func (adm Admin) ResourceExternalView(cluster string, resource string) (*model.ExternalView, error) {
	return nil, helix.ErrNotImplemented
}

func (adm Admin) ResourceIdealState(cluster, resource string) (*model.IdealState, error) {
	return nil, helix.ErrNotImplemented
}

func (adm Admin) ResourcesWithTag(cluster, tag string) ([]string, error) {
	return nil, helix.ErrNotImplemented
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
	participantConfig := kb.participantConfig(node)
	exists, err := adm.Exists(participantConfig)
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
	n.SetSimpleField("HELIX_ENABLED", "true")
	return any(
		adm.CreatePersistentRecord(participantConfig, n),
		adm.CreateEmptyPersistent(kb.instance(node)),
		adm.CreateEmptyPersistent(kb.messages(node)),
		adm.CreateEmptyPersistent(kb.currentStates(node)),
		adm.CreateEmptyPersistent(kb.errorsR(node)),
		adm.CreateEmptyPersistent(kb.statusUpdates(node)),
		adm.CreateEmptyPersistent(kb.healthReport(node)),
	)
}

func (adm Admin) DropInstance(cluster string, ic model.InstanceConfig) error {
	kb := keyBuilder{clusterID: cluster}
	if err := adm.Delete(kb.participantConfig(ic.Node())); err != nil {
		return err
	}

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
	if !option.Valid() {
		return helix.ErrInvalidAddResourceOption
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
		is.SetBucketSize(option.BucketSize)
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

	return adm.CreatePersistentRecord(kb.idealStateForResource(resource), is)
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

func (adm Admin) EnableResource(cluster string, resource string, enabled bool) error {
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
	kb := keyBuilder{clusterID: cluster}
	return adm.Children(kb.idealStates())
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

	return adm.GetRecord(instanceCfg)
}

func (adm Admin) Instances(cluster string) ([]string, error) {
	kb := keyBuilder{clusterID: cluster}
	return adm.Children(kb.instances())
}

func (adm Admin) AddStateModelDef(cluster string, stateModel string, definition *model.StateModelDef) error {
	kb := keyBuilder{clusterID: cluster}
	return adm.CreatePersistentRecord(kb.stateModelDef(stateModel), definition)
}

func (adm Admin) StateModelDefs(cluster string) ([]string, error) {
	kb := keyBuilder{clusterID: cluster}
	return adm.Children(kb.stateModelDefs())
}

func (adm Admin) StateModelDef(cluster, stateModel string) (*model.StateModelDef, error) {
	return nil, helix.ErrNotImplemented
}

// TODO
func (adm Admin) EnableInstance(cluster, instanceName string, yes bool) error {
	return nil
}

// TODO
func (adm Admin) SetResourceIdealState(cluster, instanceName string, is *model.IdealState) error {
	return nil
}

// TODO just set ideal state of the resource
func (adm Admin) Rebalance(cluster string, resource string, replica int) error {
	err, errCh := execCommand(fmt.Sprintf("%s/bin/helix-admin.sh", adm.helixInstallPath),
		"--zkSvr", adm.ZkSvr(), "--rebalance", cluster, resource, strconv.Itoa(replica))
	if err != nil {
		return err
	}

	return <-errCh
}

func (adm Admin) EnablePartitions(cluster, resource string, partitions []string, yes bool) error {
	return helix.ErrNotImplemented
}

func (adm Admin) AddConstaint() {
	// TODO
}

func (adm Admin) RemoveConstaint() {
	// TODO
}

func (adm Admin) Constraints() {
	// TODO
}
