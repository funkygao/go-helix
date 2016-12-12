package zk

import (
	"fmt"
	"testing"
	"time"

	"github.com/funkygao/assert"
	"github.com/funkygao/go-zookeeper/zk"
)

type dummyStateListener struct {
	t *testing.T
}

func (l *dummyStateListener) HandleStateChanged(state zk.State) {
	l.t.Logf("new state %s", state)
}

func (l *dummyStateListener) HandleNewSession() {
	l.t.Logf("new session")
}

func TestConnectionWithChroot(t *testing.T) {
	t.SkipNow() // TODO
}

func TestConnectionWaitUntil(t *testing.T) {
	c := newConnection(testZkSvr)
	err := c.Connect()
	assert.Equal(t, nil, err)

	t1 := time.Now()
	err = c.waitUntilConnected(0)
	assert.Equal(t, nil, err)
	t.Logf("waitUntilConnect %s", time.Since(t1))

	c.Disconnect()
}

func TestConnectSubscribeStateChanges(t *testing.T) {
	c := newConnection(testZkSvr)
	l := &dummyStateListener{t: t}
	// subscribe before connect
	c.SubscribeStateChanges(l)
	err := c.Connect()
	assert.Equal(t, nil, err)
	c.waitUntilConnected(0)
	c.Disconnect()
}

func TestConnectionEnsurePath(t *testing.T) {
	conn := newConnection(testZkSvr)
	err := conn.Connect()
	assert.Equal(t, nil, err)
	defer conn.Disconnect()

	now := time.Now().Local()
	cluster := "gohelix_connection_test_" + now.Format("20060102150405")
	p := fmt.Sprintf("/%s/a/b/c", cluster)

	err = conn.ensurePathExists(p)
	if err != nil {
		t.Error(err.Error())
	}
	defer conn.DeleteTree(p)

	exists, err := conn.Exists(p)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, exists)
}

func TestConnectionCRUD(t *testing.T) {
	c := newConnection(testZkSvr)
	err := c.Connect()
	assert.Equal(t, nil, err)

	root := "test_zk_connection"
	defer c.DeleteTree(root)
	assert.Equal(t, nil, c.CreateEphemeral(root, nil))

	// TODO more test cases
}
