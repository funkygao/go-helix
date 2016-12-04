package zk

import (
	"github.com/funkygao/go-metrics"
)

func init() {
	metrics.NewRegisteredCounter("foo", nil)
}
