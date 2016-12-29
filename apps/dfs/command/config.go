package command

import (
	"github.com/funkygao/go-helix"
)

const (
	zkSvr            = "localhost:2181"
	cluster          = "dfs"
	stateModel       = helix.StateModelMasterSlave
	resource         = "blobs"
	partitions       = 3
	replicas         = 2
	helixInstallBase = "/opt/helix"
)
