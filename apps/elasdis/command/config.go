package command

import (
	"github.com/funkygao/go-helix"
)

const (
	zkSvr            = "localhost:2181"
	cluster          = "foobar"
	stateModel       = helix.StateModelMasterSlave
	resource         = "redis"
	partitions       = 3
	replicas         = "2"
	helixInstallBase = "/opt/helix"
	redisServer      = "/opt/app/redis3/redis-server"
)
