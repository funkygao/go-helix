package helix

import (
	"testing"
	"time"
)

func TestCreateTestCluster(t *testing.T) {
	t.Parallel()

	now := time.Now().Local()
	cluster := "UtilTest_TestCreateTestCluster_" + now.Format("20060102150405")

	if testing.Short() {
		t.Skip("Skip TestCreateTestCluster")
	}

	if err := AddTestCluster(cluster); err != nil {
		t.Error(err.Error())
	}

	if err := DropTestCluster(cluster); err != nil {
		t.Error(err.Error())
	}
}
