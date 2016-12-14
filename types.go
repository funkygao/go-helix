package helix

import (
	"github.com/funkygao/go-helix/model"
)

const (
	ConstaintTypeState   = "STATE_CONSTRAINT"
	ConstaintTypeMessage = "MESSAGE_CONSTRAINT"
)

const (
	MessageStateNew           = "NEW"
	MessageStateRead          = "READ"
	MessageStateUnprocessable = "UNPROCESSABLE"
)

type HelixDefinedState string

const (
	HelixDefinedStateOnline  HelixDefinedState = "ONLINE"
	HelixDefinedStateOffline HelixDefinedState = "OFFLINE"

	HelixDefinedStateLeader  HelixDefinedState = "LEADER"
	HelixDefinedStateStandby HelixDefinedState = "STANDBY"

	HelixDefinedStateMaster HelixDefinedState = "MASTER"
	HelixDefinedStateSlave  HelixDefinedState = "SLAVE"

	// The DROPPED state is used to signify a replica that was served by a given participant, but is no longer served.
	HelixDefinedStateDropped HelixDefinedState = "DROPPED"

	// The ERROR state is used whenever the participant serving a partition encountered an error and cannot continue to serve the partition.
	HelixDefinedStateError HelixDefinedState = "ERROR"
)

type PreConnectCallback func()

type InstanceType string

const (
	InstanceTypeParticipant           InstanceType = "PARTICIPANT"
	InstanceTypeSpectator             InstanceType = "SPECTATOR"
	InstanceTypeControllerStandalone  InstanceType = "CONTROLLER"
	InstanceTypeControllerDistributed InstanceType = "CONTROLLER_PARTICIPANT"
	InstanceTypeAdministrator         InstanceType = "ADMINISTRATOR"
)

func (it InstanceType) IsParticipant() bool {
	return it == InstanceTypeParticipant
}

func (it InstanceType) IsSpectator() bool {
	return it == InstanceTypeSpectator
}

func (it InstanceType) IsController() bool {
	return it.IsControllerStandalone() || it.IsControllerDistributed()
}

func (it InstanceType) IsControllerStandalone() bool {
	return it == InstanceTypeControllerStandalone
}

func (it InstanceType) IsControllerDistributed() bool {
	return it == InstanceTypeControllerDistributed
}

type HelixConfigScope string

const (
	ConfigScopeCluster     HelixConfigScope = "CLUSTER"
	ConfigScopeParticipant HelixConfigScope = "PARTICIPANT"
	ConfigScopeResource    HelixConfigScope = "RESOURCE"
	ConfigScopeConstraint  HelixConfigScope = "CONSTRAINT"
	ConfigScopePartition   HelixConfigScope = "PARTITION"
)

type ChangeNotificationType uint8

const (
	ExternalViewChanged       ChangeNotificationType = 0
	LiveInstanceChanged       ChangeNotificationType = 1
	IdealStateChanged         ChangeNotificationType = 2
	CurrentStateChanged       ChangeNotificationType = 3
	InstanceConfigChanged     ChangeNotificationType = 4
	ControllerMessagesChanged ChangeNotificationType = 5
	InstanceMessagesChanged   ChangeNotificationType = 6
)

var changeNotificationText = map[ChangeNotificationType]string{
	ExternalViewChanged:       "ExternalView",
	LiveInstanceChanged:       "LiveInstance",
	IdealStateChanged:         "IdealState",
	CurrentStateChanged:       "CurrentState",
	InstanceConfigChanged:     "InstanceConfig",
	ControllerMessagesChanged: "ControllerMessage",
	InstanceMessagesChanged:   "InstanceMessage",
}

func ChangeNotificationText(t ChangeNotificationType) string {
	return changeNotificationText[t]
}

type ChangeNotification struct {
	ChangeType ChangeNotificationType
	ChangeData interface{}
}

type (
	// ExternalViewChangeListener is triggered when the external view is updated
	ExternalViewChangeListener func(externalViews []*model.Record, context *Context)

	// LiveInstanceChangeListener is triggered when live instances of the cluster are updated
	LiveInstanceChangeListener func(liveInstances []*model.Record, context *Context)

	// CurrentStateChangeListener is triggered when the current state of a participant changed
	CurrentStateChangeListener func(instance string, currentState []*model.Record, context *Context)

	// IdealStateChangeListener is triggered when the ideal state changed
	IdealStateChangeListener func(idealState []*model.Record, context *Context)

	// InstanceConfigChangeListener is triggered when the instance configs are updated
	InstanceConfigChangeListener func(configs []*model.Record, context *Context)

	// ControllerMessageListener is triggered when the controller messages are updated
	ControllerMessageListener func(messages []*model.Record, context *Context)

	// MessageListener is triggered when the instance received new messages
	MessageListener func(instance string, messages []*model.Record, context *Context)
)

const (
	MessageTypeStateTransition  = "STATE_TRANSITION"
	MessageTypeScheduler        = "SCHEDULER_MSG"
	MessageTypeUserDefine       = "USER_DEFINE_MSG"
	MessageTypeController       = "USER_DEFINE_MSG"
	MessageTypeTaskReply        = "TASK_REPLY"
	MessageTypeNoOp             = "NO_OP"
	MessageTypeParticipantError = "PARTICIPANT_ERROR_REPORT"
)

const (
	RebalancerModeFullAuto    = "FULL_AUTO"
	RebalancerModeSemiAuto    = "SEMI_AUTO"
	RebalancerModeCustomized  = "CUSTOMIZED"
	RebalancerModeUserDefined = "USER_DEFINED"
	RebalancerModeTask        = "TASK"
)

const (
	StateModelLeaderStandby      = "LeaderStandby"
	StateModelMasterSlave        = "MasterSlave"
	StateModelOnlineOffline      = "OnlineOffline"
	StateModelDefaultSchemata    = "STORAGE_DEFAULT_SM_SCHEMATA"
	StateModelSchedulerTaskQueue = "SchedulerTaskQueue"
	StateModelTask               = "Task"
)

var HelixDefaultStateModels = map[string][]byte{
	StateModelLeaderStandby: []byte(`
{
  "id" : "LeaderStandby",
  "mapFields" : {
    "DROPPED.meta" : {
      "count" : "-1"
    },
    "LEADER.meta" : {
      "count" : "1"
    },
    "LEADER.next" : {
      "DROPPED" : "STANDBY",
      "STANDBY" : "STANDBY",
      "OFFLINE" : "STANDBY"
    },
    "OFFLINE.meta" : {
      "count" : "-1"
    },
    "OFFLINE.next" : {
      "DROPPED" : "DROPPED",
      "STANDBY" : "STANDBY",
      "LEADER" : "STANDBY"
    },
    "STANDBY.meta" : {
      "count" : "R"
    },
    "STANDBY.next" : {
      "DROPPED" : "OFFLINE",
      "OFFLINE" : "OFFLINE",
      "LEADER" : "LEADER"
    }
  },
  "listFields" : {
    "STATE_PRIORITY_LIST" : [ "LEADER", "STANDBY", "OFFLINE", "DROPPED" ],
    "STATE_TRANSITION_PRIORITYLIST" : [ "LEADER-STANDBY", "STANDBY-LEADER", "OFFLINE-STANDBY", "STANDBY-OFFLINE", "OFFLINE-DROPPED" ]
  },
  "simpleFields" : {
    "INITIAL_STATE" : "OFFLINE"
  }
}
`),

	StateModelMasterSlave: []byte(`
{
  "id" : "MasterSlave",
  "mapFields" : {
    "DROPPED.meta" : {
      "count" : "-1"
    },
    "ERROR.meta" : {
      "count" : "-1"
    },
    "ERROR.next" : {
      "DROPPED" : "DROPPED",
      "OFFLINE" : "OFFLINE"
    },
    "MASTER.meta" : {
      "count" : "1"
    },
    "MASTER.next" : {
      "SLAVE" : "SLAVE",
      "DROPPED" : "SLAVE",
      "OFFLINE" : "SLAVE"
    },
    "OFFLINE.meta" : {
      "count" : "-1"
    },
    "OFFLINE.next" : {
      "SLAVE" : "SLAVE",
      "DROPPED" : "DROPPED",
      "MASTER" : "SLAVE"
    },
    "SLAVE.meta" : {
      "count" : "R"
    },
    "SLAVE.next" : {
      "DROPPED" : "OFFLINE",
      "OFFLINE" : "OFFLINE",
      "MASTER" : "MASTER"
    }
  },
  "listFields" : {
    "STATE_PRIORITY_LIST" : [ "MASTER", "SLAVE", "OFFLINE", "DROPPED", "ERROR" ],
    "STATE_TRANSITION_PRIORITYLIST" : [ "MASTER-SLAVE", "SLAVE-MASTER", "OFFLINE-SLAVE", "SLAVE-OFFLINE", "OFFLINE-DROPPED" ]
  },
  "simpleFields" : {
    "INITIAL_STATE" : "OFFLINE"
  }
}
`),

	StateModelOnlineOffline: []byte(`
{
  "id" : "OnlineOffline",
  "mapFields" : {
    "DROPPED.meta" : {
      "count" : "-1"
    },
    "OFFLINE.meta" : {
      "count" : "-1"
    },
    "OFFLINE.next" : {
      "DROPPED" : "DROPPED",
      "ONLINE" : "ONLINE"
    },
    "ONLINE.meta" : {
      "count" : "R"
    },
    "ONLINE.next" : {
      "DROPPED" : "OFFLINE",
      "OFFLINE" : "OFFLINE"
    }
  },
  "listFields" : {
    "STATE_PRIORITY_LIST" : [ "ONLINE", "OFFLINE", "DROPPED" ],
    "STATE_TRANSITION_PRIORITYLIST" : [ "OFFLINE-ONLINE", "ONLINE-OFFLINE", "OFFLINE-DROPPED" ]
  },
  "simpleFields" : {
    "INITIAL_STATE" : "OFFLINE"
  }
}	
`),

	StateModelDefaultSchemata: []byte(`
{
  "id" : "STORAGE_DEFAULT_SM_SCHEMATA",
  "mapFields" : {
    "DROPPED.meta" : {
      "count" : "-1"
    },
    "ERROR.meta" : {
      "count" : "-1"
    },
    "ERROR.next" : {
      "DROPPED" : "DROPPED",
      "OFFLINE" : "OFFLINE"
    },
    "MASTER.meta" : {
      "count" : "N"
    },
    "MASTER.next" : {
      "DROPPED" : "OFFLINE",
      "OFFLINE" : "OFFLINE"
    },
    "OFFLINE.meta" : {
      "count" : "-1"
    },
    "OFFLINE.next" : {
      "DROPPED" : "DROPPED",
      "MASTER" : "MASTER"
    }
  },
  "listFields" : {
    "STATE_PRIORITY_LIST" : [ "MASTER", "OFFLINE", "DROPPED", "ERROR" ],
    "STATE_TRANSITION_PRIORITYLIST" : [ "MASTER-OFFLINE", "OFFLINE-MASTER" ]
  },
  "simpleFields" : {
    "INITIAL_STATE" : "OFFLINE"
  }
}
`),

	StateModelSchedulerTaskQueue: []byte(`
{
  "id" : "SchedulerTaskQueue",
  "mapFields" : {
    "COMPLETED.meta" : {
      "count" : "1"
    },
    "COMPLETED.next" : {
      "DROPPED" : "DROPPED",
      "COMPLETED" : "COMPLETED"
    },
    "DROPPED.meta" : {
      "count" : "-1"
    },
    "DROPPED.next" : {
      "DROPPED" : "DROPPED"
    },
    "OFFLINE.meta" : {
      "count" : "-1"
    },
    "OFFLINE.next" : {
      "DROPPED" : "DROPPED",
      "OFFLINE" : "OFFLINE",
      "COMPLETED" : "COMPLETED"
    }
  },
  "listFields" : {
    "STATE_PRIORITY_LIST" : [ "COMPLETED", "OFFLINE", "DROPPED" ],
    "STATE_TRANSITION_PRIORITYLIST" : [ "OFFLINE-COMPLETED", "OFFLINE-DROPPED", "COMPLETED-DROPPED" ]
  },
  "simpleFields" : {
    "INITIAL_STATE" : "OFFLINE"
  }
}
`),

	StateModelTask: []byte(`
{
  "id" : "Task",
  "mapFields" : {
    "COMPLETED.meta" : {
      "count" : "-1"
    },
    "COMPLETED.next" : {
      "STOPPED" : "INIT",
      "DROPPED" : "DROPPED",
      "RUNNING" : "INIT",
      "INIT" : "INIT",
      "COMPLETED" : "COMPLETED",
      "TASK_ERROR" : "INIT",
      "TIMED_OUT" : "INIT"
    },
    "DROPPED.meta" : {
      "count" : "-1"
    },
    "DROPPED.next" : {
      "DROPPED" : "DROPPED"
    },
    "INIT.meta" : {
      "count" : "-1"
    },
    "INIT.next" : {
      "STOPPED" : "RUNNING",
      "DROPPED" : "DROPPED",
      "RUNNING" : "RUNNING",
      "INIT" : "INIT",
      "COMPLETED" : "RUNNING",
      "TASK_ERROR" : "RUNNING",
      "TIMED_OUT" : "RUNNING"
    },
    "RUNNING.meta" : {
      "count" : "-1"
    },
    "RUNNING.next" : {
      "STOPPED" : "STOPPED",
      "DROPPED" : "DROPPED",
      "RUNNING" : "RUNNING",
      "INIT" : "INIT",
      "COMPLETED" : "COMPLETED",
      "TASK_ERROR" : "TASK_ERROR",
      "TIMED_OUT" : "TIMED_OUT"
    },
    "STOPPED.meta" : {
      "count" : "-1"
    },
    "STOPPED.next" : {
      "STOPPED" : "STOPPED",
      "DROPPED" : "DROPPED",
      "RUNNING" : "RUNNING",
      "INIT" : "INIT",
      "COMPLETED" : "RUNNING",
      "TASK_ERROR" : "RUNNING",
      "TIMED_OUT" : "RUNNING"
    },
    "TASK_ERROR.meta" : {
      "count" : "-1"
    },
    "TASK_ERROR.next" : {
      "STOPPED" : "INIT",
      "DROPPED" : "DROPPED",
      "RUNNING" : "INIT",
      "INIT" : "INIT",
      "COMPLETED" : "INIT",
      "TIMED_OUT" : "INIT",
      "TASK_ERROR" : "TASK_ERROR"
    },
    "TIMED_OUT.meta" : {
      "count" : "-1"
    },
    "TIMED_OUT.next" : {
      "STOPPED" : "INIT",
      "DROPPED" : "DROPPED",
      "RUNNING" : "INIT",
      "INIT" : "INIT",
      "COMPLETED" : "INIT",
      "TASK_ERROR" : "INIT",
      "TIMED_OUT" : "TIMED_OUT"
    }
  },
  "listFields" : {
    "STATE_PRIORITY_LIST" : [ "INIT", "RUNNING", "STOPPED", "COMPLETED", "TIMED_OUT", "TASK_ERROR", "DROPPED" ],
    "STATE_TRANSITION_PRIORITYLIST" : [ "INIT-RUNNING", "RUNNING-STOPPED", "RUNNING-COMPLETED", "RUNNING-TIMED_OUT", "RUNNING-TASK_ERROR", "STOPPED-RUNNING", "INIT-DROPPED", "RUNNING-DROPPED", "COMPLETED-DROPPED", "STOPPED-DROPPED", "TIMED_OUT-DROPPED", "TASK_ERROR-DROPPED", "RUNNING-INIT", "COMPLETED-INIT", "STOPPED-INIT", "TIMED_OUT-INIT", "TASK_ERROR-INIT" ]
  },
  "simpleFields" : {
    "INITIAL_STATE" : "INIT"
  }
}`),
}
