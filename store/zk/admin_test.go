// +build admin
package zk

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/funkygao/go-helix"
	"github.com/yichen/go-zookeeper/zk"
)

var (
	testZkSvr = "localhost:2181"
)

func TestNewZKHelixAdminWithOptions(t *testing.T) {

}

func TestAddAndDropCluster(t *testing.T) {
	t.Parallel()

	// definitely a new cluster name by timestamp
	now := time.Now().Local()
	cluster := "AdminTest_TestAddAndDropCluster_" + now.Format("20060102150405")

	a := Admin{zkSvr: testZkSvr}
	err := a.AddCluster(cluster)
	if err != nil {
		t.Error(err)
	}

	// if cluster is already added, add it again and it should return ErrNodeAlreadyExists
	err = a.AddCluster(cluster)
	if err != helix.ErrNodeAlreadyExists {
		t.Error(err)
	}

	// listClusters
	clusters, err := a.Clusters()
	t.Logf("%+v%+v", clusters, err)
	if err != nil {
		t.Error("Expect OK")
	}
	if !strSliceContains(clusters, cluster) {
		t.Error("Expect OK")
	}

	a.DropCluster(cluster)
	clusters, err = a.Clusters()
	if err != nil || strSliceContains(clusters, cluster) {
		t.Error("Expect dropped")
	}
}

func TestAddCluster(t *testing.T) {
	t.Parallel()

	now := time.Now().Local()
	cluster := "AdminTest_TestAddCluster" + now.Format("20060102150405")

	a := Admin{zkSvr: testZkSvr}
	err := a.AddCluster(cluster)
	if err == nil {
		defer a.DropCluster(cluster)
	}

	// verify the data structure in zookeeper
	propertyStore := fmt.Sprintf("/%s/PROPERTYSTORE", cluster)
	verifyNodeExist(t, propertyStore)

	stateModelDefs := fmt.Sprintf("/%s/STATEMODELDEFS", cluster)
	verifyNodeExist(t, stateModelDefs)
	verifyChildrenCount(t, stateModelDefs, 6)

	instances := fmt.Sprintf("/%s/INSTANCES", cluster)
	verifyNodeExist(t, instances)

	configs := fmt.Sprintf("/%s/CONFIGS", cluster)
	verifyNodeExist(t, configs)
	verifyChildrenCount(t, configs, 3)

	idealStates := fmt.Sprintf("/%s/IDEALSTATES", cluster)
	verifyNodeExist(t, idealStates)

	externalView := fmt.Sprintf("/%s/EXTERNALVIEW", cluster)
	verifyNodeExist(t, externalView)

	liveInstances := fmt.Sprintf("/%s/LIVEINSTANCES", cluster)
	verifyNodeExist(t, liveInstances)

	controller := fmt.Sprintf("/%s/CONTROLLER", cluster)
	verifyNodeExist(t, controller)
	verifyChildrenCount(t, controller, 4)

}

func TestSetConfig(t *testing.T) {
	t.Parallel()

	now := time.Now().Local()
	cluster := "AdminTest_TestSetConfig_" + now.Format("20060102150405")

	a := Admin{zkSvr: testZkSvr}
	err := a.AddCluster(cluster)
	if err == nil {
		defer a.DropCluster(cluster)
	}

	property := map[string]string{
		"allowParticipantAutoJoin": "true",
	}

	a.SetConfig(cluster, "CLUSTER", property)

	prop, _ := a.GetConfig(cluster, "CLUSTER", []string{"allowParticipantAutoJoin"})

	if prop["allowParticipantAutoJoin"] != "true" {
		t.Error("allowParticipantAutoJoin config set/get failed")
	}
}

func TestAddDropNode(t *testing.T) {
	t.Parallel()

	// verify not able to add node before cluster is setup
	now := time.Now().Local()
	cluster := "AdminTest_TestAddDropNode_" + now.Format("20060102150405")

	a := Admin{zkSvr: testZkSvr}
	node := "localhost_19932"

	// add node before adding cluster, expect fail
	if err := a.AddNode(cluster, node); err != helix.ErrClusterNotSetup {
		t.Error("Must error out for AddNode if cluster not setup")
	}

	// now add the cluster and add the node again
	a.AddCluster(cluster)
	defer a.DropCluster(cluster)

	if err := a.AddNode(cluster, node); err != nil {
		t.Error("Should be able to add node")
	}

	// add the same node again, should expect error ErrNodeAlreadyExists
	if err := a.AddNode(cluster, node); err != helix.ErrNodeAlreadyExists {
		t.Error("should not be able to add the same node")
	}

	// listInstanceInfo
	if info, err := a.ListInstanceInfo(cluster, node); err != nil || info == "" || !strings.Contains(info, node) {
		t.Error("expect OK")
	}

	// drop the node
	if err := a.DropNode(cluster, node); err != nil {
		t.Error("failed to drop cluster node")
	}
	// listInstanceInfo
	if _, err := a.ListInstanceInfo(cluster, node); err != helix.ErrNodeNotExist {
		t.Error("expect OK")
	}

	// drop node again and we should see an error ErrNodeNotExist
	if err := a.DropNode(cluster, node); err != helix.ErrNodeNotExist {
		t.Error("failed to see expected error ErrNodeNotExist")
	}

	// make sure the path does not exist in zookeeper
	verifyNodeNotExist(t, fmt.Sprintf("/%s/INSTANCES/%s", cluster, node))
	verifyNodeNotExist(t, fmt.Sprintf("/%s/CONFIGS/PARTICIPANT/%s", cluster, node))
}

func TestAddDropResource(t *testing.T) {
	t.Parallel()

	now := time.Now().Local()
	cluster := "AdminTest_TestAddResource_" + now.Format("20060102150405")
	resource := "resource"

	a := Admin{zkSvr: testZkSvr}

	// expect error if cluster not setup
	if err := a.AddResource(cluster, resource, helix.DefaultAddResourceOption(32, "MasterSlave")); err != helix.ErrClusterNotSetup {
		t.Error("must setup cluster before addResource")
	}
	if err := a.DropResource(cluster, resource); err != helix.ErrClusterNotSetup {
		t.Error("must setup cluster before addResource")
	}
	if _, err := a.ListResources(cluster); err != helix.ErrClusterNotSetup {
		t.Error("must setup cluster")
	}

	a.AddCluster(cluster)
	defer a.DropCluster(cluster)

	// it is ok to dropResource before resource exists
	if err := a.DropResource(cluster, resource); err != nil {
		t.Error("expect OK")
	}

	// expect error if state model does not exist
	if err := a.AddResource(cluster, resource, helix.DefaultAddResourceOption(32, "NotExistStateModel")); err != helix.ErrStateModelDefNotExist {
		t.Error("must use valid state model")
	}

	// expect pass
	if err := a.AddResource(cluster, resource, helix.DefaultAddResourceOption(32, "MasterSlave")); err != nil {
		t.Error("fail addResource")
	}
	if info, err := a.ListResources(cluster); err != nil || info == "" {
		t.Error("expect OK")
	}

	kb := keyBuilder{cluster}
	isPath := kb.idealStates() + "/resource"
	verifyNodeExist(t, isPath)

	if err := a.DropResource(cluster, resource); err != nil {
		t.Error("expect OK")
	}

	verifyNodeNotExist(t, isPath)
}

func TestEnableDisableResource(t *testing.T) {
	t.Parallel()

	now := time.Now().Local()
	cluster := "AdminTest_TestEnableDisableResource_" + now.Format("20060102150405")
	resource := "resource"

	a := Admin{zkSvr: testZkSvr}

	// expect error if cluster not setup
	if err := a.EnableResource(cluster, resource); err != helix.ErrClusterNotSetup {
		t.Error("must setup cluster before enableResource")
	}
	if err := a.DisableResource(cluster, resource); err != helix.ErrClusterNotSetup {
		t.Error("must setup cluster before enableResource")
	}

	a.AddCluster(cluster)
	defer a.DropCluster(cluster)

	// expect error if resource not exist
	if err := a.EnableResource(cluster, resource); err != helix.ErrResourceNotExists {
		t.Error("expect ErrResourceNotExists")
	}
	if err := a.DisableResource(cluster, resource); err != helix.ErrResourceNotExists {
		t.Error("expect ErrResourceNotExists")
	}
	if err := a.AddResource(cluster, "resource", helix.DefaultAddResourceOption(32, "MasterSlave")); err != nil {
		t.Error("fail addResource")
	}
	if err := a.EnableResource(cluster, resource); err != nil {
		// expect error if resource not exist
		t.Error("expect OK")
	}

	kb := keyBuilder{cluster}
	path := kb.idealStates() + "/resource"

	conn := newConnection(testZkSvr)
	err := conn.Connect()
	if err != nil {
		t.Error("Failed to connect to test zookeeper")
	}
	defer conn.Disconnect()

	if enabled := conn.GetSimpleFieldBool(path, "HELIX_ENABLED"); !enabled {
		t.Error("resource not enabled")
	}

	// disable resource
	if err := a.DisableResource(cluster, resource); err != nil {
		t.Error("expect OK")
	}

	if enabled := conn.GetSimpleFieldBool(path, "HELIX_ENABLED"); enabled {
		t.Error("resource not disabled")
	}
}

func connectLocalZk(t *testing.T) *zk.Conn {
	zkServers := strings.Split(testZkSvr, ",")
	conn, _, err := zk.Connect(zkServers, time.Second)
	if err != nil {
		t.FailNow()
	}

	return conn
}

func verifyNodeExist(t *testing.T, path string) {
	conn := connectLocalZk(t)
	defer conn.Close()

	if exists, _, err := conn.Exists(path); err != nil || !exists {
		t.Error("failed verifyNodeExist")
	}
}

func verifyNodeNotExist(t *testing.T, path string) {
	conn := connectLocalZk(t)
	defer conn.Close()

	if exists, _, err := conn.Exists(path); err != nil || exists {
		t.Error("failed verifyNotNotExist")
		t.FailNow()
	}
}

func verifyChildrenCount(t *testing.T, path string, count int32) {
	conn := connectLocalZk(t)
	defer conn.Close()

	_, stat, err := conn.Get(path)
	if err != nil {
		t.FailNow()
	}

	if stat.NumChildren != count {
		t.Errorf("Node %s should have %d children, but only have %d children", path, count, stat.NumChildren)
	}
}

func strSliceContains(a []string, s string) bool {
	for _, ele := range a {
		if ele == s {
			return true
		}
	}

	return false
}
