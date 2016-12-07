package zk

import (
	"github.com/funkygao/go-metrics"
)

type metricsReporter struct {
}

func newMetricsReporter() *metricsReporter {
	return &metricsReporter{}
}

func init() {
	metrics.NewRegisteredCounter("foo", nil)
}
