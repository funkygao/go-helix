package rebalancer

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
)

type Rebalancer interface {
	Init(helix.HelixManager)

	// ComputeNewIdealState provides all the relevant information needed to rebalance a resource.
	ComputeNewIdealState(resourceName string, currentIdealState *model.IdealState) *model.IdealState
}
