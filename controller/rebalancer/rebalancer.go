// Package rebalancer is an abstraction of Helix rebalancer.
package rebalancer

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
)

type Rebalancer interface {
	Init(helix.HelixManager)

	// ComputeNewIdealState provides all the relevant information needed to rebalance a resource.
	ComputeNewIdealState(resource string, currentIdealState *model.IdealState) *model.IdealState
}
