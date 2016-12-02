package helix

type HelixConfigScope string

type ChangeNotificationType uint8

type ChangeNotification struct {
	ChangeType ChangeNotificationType
	ChangeData interface{}
}

type (
	// ExternalViewChangeListener is triggered when the external view is updated
	ExternalViewChangeListener func(externalViews []*Record, context *Context)

	// LiveInstanceChangeListener is triggered when live instances of the cluster are updated
	LiveInstanceChangeListener func(liveInstances []*Record, context *Context)

	// CurrentStateChangeListener is triggered when the current state of a participant changed
	CurrentStateChangeListener func(instance string, currentState []*Record, context *Context)

	// IdealStateChangeListener is triggered when the ideal state changed
	IdealStateChangeListener func(idealState []*Record, context *Context)

	// InstanceConfigChangeListener is triggered when the instance configs are updated
	InstanceConfigChangeListener func(configs []*Record, context *Context)

	// ControllerMessageListener is triggered when the controller messages are updated
	ControllerMessageListener func(messages []*Record, context *Context)

	// MessageListener is triggered when the instance received new messages
	MessageListener func(instance string, messages []*Record, context *Context)
)
