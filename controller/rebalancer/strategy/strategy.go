package strategy

import (
	"github.com/funkygao/go-helix/model"
)

// RebalanceStrategy computes the assignment of partition->instance.
type RebalanceStrategy interface {

	// Init perform the necessary initialization for the rebalance strategy object.
	Init(resourceName string, partitions []string, states map[string]int, maximumPerNode int)

	// PartitionAssignment compute the preference lists and (optional partition-state mapping) for the given resource.
	PartitionAssignment(instances []string, liveInstances []string,
		currentMapping map[string]map[string]string) *model.Record
}
