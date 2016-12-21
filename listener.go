package helix

import (
	"github.com/funkygao/go-helix/model"
)

type (
	// ExternalViewChangeListener is triggered when the external view is updated.
	ExternalViewChangeListener func(externalViews []*model.ExternalView, ctx *Context)

	// LiveInstanceChangeListener is triggered when live instances of the cluster are updated.
	LiveInstanceChangeListener func(liveInstances []*model.LiveInstance, ctx *Context)

	// CurrentStateChangeListener is triggered when the current state of a participant changed.
	CurrentStateChangeListener func(instance string, currentState []*model.CurrentState, ctx *Context)

	// IdealStateChangeListener is triggered when the ideal state changed.
	IdealStateChangeListener func(idealState []*model.IdealState, ctx *Context)

	// InstanceConfigChangeListener is triggered when the instance configs are updated.
	InstanceConfigChangeListener func(configs []*model.InstanceConfig, ctx *Context)

	// MessageListener is triggered when the instance received new messages.
	MessageListener func(instance string, messages []*model.Message, ctx *Context)

	// ControllerChangeListener is triggered when controller changes.
	ControllerChangeListener func(ctx *Context)
)
