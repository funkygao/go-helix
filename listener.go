package helix

import (
	"github.com/funkygao/go-helix/model"
)

type (
	// ExternalViewChangeListener is triggered when the external view is updated.
	ExternalViewChangeListener func(externalViews []*model.Record, context *Context)

	// LiveInstanceChangeListener is triggered when live instances of the cluster are updated.
	LiveInstanceChangeListener func(liveInstances []*model.Record, context *Context)

	// CurrentStateChangeListener is triggered when the current state of a participant changed.
	CurrentStateChangeListener func(instance string, currentState []*model.Record, context *Context)

	// IdealStateChangeListener is triggered when the ideal state changed.
	IdealStateChangeListener func(idealState []*model.Record, context *Context)

	// InstanceConfigChangeListener is triggered when the instance configs are updated.
	InstanceConfigChangeListener func(configs []*model.Record, context *Context)

	// MessageListener is triggered when the instance received new messages.
	MessageListener func(instance string, messages []*model.Record, context *Context)

	// ControllerMessageListener is triggered when the controller messages are updated.
	ControllerMessageListener func(messages []*model.Record, context *Context)

	// ControllerChangeListener is triggered when controller changes.
	ControllerChangeListener func(context *Context)
)
