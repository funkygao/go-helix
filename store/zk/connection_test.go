package zk

import (
	"fmt"
	"testing"
	"time"
)

func TestEnsurePath(t *testing.T) {
	conn := newConnection(testZkSvr)
	err := conn.Connect()
	if err != nil {
		t.Error(err.Error())
	}
	defer conn.Disconnect()

	now := time.Now().Local()
	cluster := "gohelix_connection_test_" + now.Format("20060102150405")
	p := fmt.Sprintf("/%s/a/b/c", cluster)

	err = conn.ensurePathExists(p)
	if err != nil {
		t.Error(err.Error())
	}
	defer conn.DeleteTree(cluster)

	exists, err := conn.Exists(p)
	if err != nil {
		t.Error(err.Error())
	}
	if !exists {
		t.Error("should exist the path:" + p)
	}

}
