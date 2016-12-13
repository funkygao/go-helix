package zk

import (
	"fmt"
	"io/ioutil"
	glog "log"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/funkygao/assert"
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/zkclient"
	"github.com/yichen/go-zookeeper/zk"
)

var (
	testZkSvr   = "localhost:2181"
	testCluster = "foobar"
)

func init() {
	log.Disable()
	glog.SetOutput(ioutil.Discard)
	go http.ListenAndServe(":10008", nil)
}

func TestAdminMultipleConnect(t *testing.T) {
	t.Parallel()

	adm := NewZkHelixAdmin(testZkSvr, zkclient.WithSessionTimeout(time.Second),
		zkclient.WithoutRetry())

	// parallel connect
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()

			assert.Equal(t, nil, adm.Connect())
		}(&wg, i)
	}
	wg.Wait()

	// parallel disconnect
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()

			adm.Disconnect()
		}(&wg, i)
	}
	wg.Wait()
}

func TestAdminRebalance(t *testing.T) {
	adm := NewZkHelixAdmin(testZkSvr)
	assert.Equal(t, nil, adm.Connect())

	adm.Rebalance(testCluster, "resource", 3)
	t.SkipNow()
}

func TestZKHelixAdminBasics(t *testing.T) {
	t.Parallel()

	adm := NewZkHelixAdmin(testZkSvr)
	assert.Equal(t, nil, adm.Connect())

	clusters, err := adm.Clusters()
	assert.Equal(t, nil, err)
	t.Logf("clusters: %+v", clusters)

	now := time.Now()
	cluster := "test_cluster" + now.Format("20060102150405")

	// cluster CRUD
	err = adm.AddCluster(cluster)
	assert.Equal(t, nil, err)
	assert.Equal(t, nil, adm.DropCluster(cluster))

	// prepare the cluster
	err = adm.AddCluster(cluster)
	assert.Equal(t, nil, err)
	defer adm.DropCluster(cluster)

	err = adm.AllowParticipantAutoJoin(cluster, true)
	assert.Equal(t, nil, err)
	err = adm.AllowParticipantAutoJoin(cluster, false)
	assert.Equal(t, nil, err)

	// EnableCluster
	assert.Equal(t, nil, adm.EnableCluster(cluster, true))
	assert.Equal(t, nil, adm.EnableCluster(cluster, false))

	// AddInstance
	record := model.NewRecord("localhost_10965")
	ic := model.NewInstanceConfigFromRecord(record)
	ic.SetHost("localhost")
	ic.SetPort("10965")
	assert.Equal(t, nil, adm.AddInstance(cluster, ic))

	// Instances
	instances, err := adm.Instances(cluster)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(instances))
	t.Logf("instances: %+v", instances)

	// InstanceConfig
	ic1, err := adm.InstanceConfig(cluster, ic.InstanceName())
	assert.Equal(t, ic.Host(), ic1.Host())
	assert.Equal(t, ic.Port(), ic1.Port())

	// InstanceInfo
	inf, err := adm.InstanceInfo(cluster, ic)
	assert.Equal(t, nil, err)
	t.Logf("instance info: %+v", inf)

	// AddInstanceTag
	assert.Equal(t, nil, adm.AddInstanceTag(cluster, ic.Node(), "tag"))
	// InstancesWithTag
	ins, err := adm.InstancesWithTag(cluster, "tag")
	assert.Equal(t, nil, err)
	assert.Equal(t, ic.InstanceName(), ins[0])
	// RemoveIntanceTag
	assert.Equal(t, nil, adm.RemoveInstanceTag(cluster, ic.Node(), "tag"))
	ins, err = adm.InstancesWithTag(cluster, "tag")
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(ins))

	// DropInstance
	assert.Equal(t, nil, adm.DropInstance(cluster, ic))

	// state model defs
	sms, err := adm.StateModelDefs(cluster)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, len(sms) > 1)
	t.Logf("%+v", sms)
	for _, sm := range sms {
		_, err := adm.StateModelDef(cluster, sm)
		assert.Equal(t, nil, err)
	}
}

func TestAdminResourceRelated(t *testing.T) {
	adm := NewZkHelixAdmin(testZkSvr,
		zkclient.WithRetryAttempts(2),
		zkclient.WithRetryBackoff(time.Millisecond),
		zkclient.WithoutRetry())
	assert.Equal(t, nil, adm.Connect())

	now := time.Now()
	cluster := "test_resource" + now.Format("20060102150405")

	resource := "redis_cluster"

	// prepare the cluster
	err := adm.AddCluster(cluster)
	assert.Equal(t, nil, err)
	//defer adm.DropCluster(cluster)

	// AddResource
	err = adm.AddResource(cluster, resource, helix.DefaultAddResourceOption(19, helix.StateModelLeaderStandby))
	assert.Equal(t, nil, err)

	// Resources
	resources, err := adm.Resources(cluster)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(resources))
	assert.Equal(t, resource, resources[0])

	// ResourcesWithTag
	resources, err = adm.ResourcesWithTag(cluster, "tag")
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(resources))

	// ResourceExternalView
	view, err := adm.ResourceExternalView(cluster, resource)
	// the external view might not exist
	t.Logf("%+v %+v", view, err)

	// ResourceIdealState
	is, err := adm.ResourceIdealState(cluster, resource)
	assert.Equal(t, nil, err)
	t.Logf("%+v", is)

	// SetResourceIdealState
	err = adm.SetResourceIdealState(cluster, resource, is)
	assert.Equal(t, nil, err)

	// EnableResource
	err = adm.EnableResource(cluster, resource, true)
	assert.Equal(t, nil, err)
	err = adm.EnableResource(cluster, resource, false)
	assert.Equal(t, nil, err)

	// DropResource
	err = adm.DropResource(cluster, resource)
	assert.Equal(t, nil, err)

}

func TestControllerHistory(t *testing.T) {
	adm := NewZkHelixAdmin(testZkSvr)
	assert.Equal(t, nil, adm.Connect())

	history, err := adm.ControllerHistory(testCluster)
	assert.Equal(t, nil, err)
	for _, h := range history {
		t.Logf("%s", h)
	}
}

func TestNewZKHelixAdminWithOptions(t *testing.T) {
	admin := NewZkHelixAdmin(testZkSvr, zkclient.WithSessionTimeout(time.Second))
	assert.Equal(t, time.Second, admin.SessionTimeout())
}

func TestAddAndDropCluster(t *testing.T) {
	t.Parallel()

	// definitely a new cluster name by timestamp
	now := time.Now().Local()
	cluster := "AdminTest_TestAddAndDropCluster_" + now.Format("20060102150405")

	a := NewZkHelixAdmin(testZkSvr)
	assert.Equal(t, nil, a.Connect())

	err := a.AddCluster(cluster)
	if err != nil {
		t.Error(err)
	}

	// if cluster is already added, add it again and it should return ErrNodeAlreadyExists
	err = a.AddCluster(cluster)
	if err != helix.ErrNodeAlreadyExists {
		t.Error(err)
	}

	clusters, err := a.Clusters()
	t.Logf("%+v %+v", clusters, err)
	if err != nil {
		t.Error("Expect OK")
	}
	if !strSliceContains(clusters, cluster) {
		t.Error("Expect OK")
	}

	err = a.DropCluster(cluster)
	assert.Equal(t, nil, err)
	clusters, err = a.Clusters()
	if err != nil || strSliceContains(clusters, cluster) {
		t.Errorf("err=%v %+v %v", err, clusters, cluster)
	}
}

func TestAddCluster(t *testing.T) {
	t.Parallel()

	now := time.Now().Local()
	cluster := "AdminTest_TestAddCluster" + now.Format("20060102150405")

	a := NewZkHelixAdmin(testZkSvr)
	err := a.AddCluster(cluster)
	assert.Equal(t, zkclient.ErrNotConnected, err)
	assert.Equal(t, nil, a.Connect())

	err = a.AddCluster(cluster)
	assert.Equal(t, nil, err)
	defer a.DropCluster(cluster)

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

	a := NewZkHelixAdmin(testZkSvr)
	assert.Equal(t, nil, a.Connect())
	err := a.AddCluster(cluster)
	if err == nil {
		defer a.DropCluster(cluster)
	}

	property := map[string]string{
		"allowParticipantAutoJoin": "true",
	}

	a.SetConfig(cluster, helix.ConfigScopeCluster, property)

	prop, _ := a.GetConfig(cluster, helix.ConfigScopeCluster, []string{"allowParticipantAutoJoin"})

	if prop["allowParticipantAutoJoin"] != "true" {
		t.Errorf("allowParticipantAutoJoin config set/get failed %+v", prop)
	}
}

func TestAddDropNode(t *testing.T) {
	t.Parallel()

	// verify not able to add node before cluster is setup
	now := time.Now().Local()
	cluster := "AdminTest_TestAddDropNode_" + now.Format("20060102150405")

	a := NewZkHelixAdmin(testZkSvr)
	assert.Equal(t, nil, a.Connect())
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

}

func connectLocalZk(t *testing.T) *zk.Conn {
	zkServers := strings.Split(testZkSvr, ",")
	conn, _, err := zk.Connect(zkServers, time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}

	return conn
}

func verifyNodeExist(t *testing.T, path string) {
	conn := connectLocalZk(t)
	defer conn.Close()

	if exists, _, err := conn.Exists(path); err != nil || !exists {
		t.Errorf("failed verifyNodeExist: %s", path)
	}
}

func verifyNodeNotExist(t *testing.T, path string) {
	conn := connectLocalZk(t)
	defer conn.Close()

	if exists, _, err := conn.Exists(path); err != nil || exists {
		t.Errorf("failed verifyNotNotExist: %s", path)
	}
}

func verifyChildrenCount(t *testing.T, path string, count int32) {
	conn := connectLocalZk(t)
	defer conn.Close()

	_, stat, err := conn.Get(path)
	if err != nil {
		t.Errorf("%v", err)
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
