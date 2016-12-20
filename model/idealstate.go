package model

import (
	"math"
	"strconv"
)

// ideal state content example:
// {
//   "id" : "MyResource",
//   "simpleFields" : {
//     "IDEAL_STATE_MODE" : "AUTO",
//     "NUM_PARTITIONS" : "6",
//     "REBALANCE_MODE" : "SEMI_AUTO",
//     "REBALANCE_STRATEGY" : "DEFAULT",
//     "REBALANCER_CLASS_NAME": "",
//     "REPLICAS" : "3",
//     "STATE_MODEL_DEF_REF" : "MyStateModel",
//     "STATE_MODEL_FACTORY_NAME" : "DEFAULT"
//   },
//   "mapFields" : {
//     "MyResource_0" : {
//       "localhost_12000" : "SLAVE",
//       "localhost_12001" : "MASTER",
//       "localhost_12002" : "SLAVE"
//     },
//     "MyResource_1" : {
//       "localhost_12000" : "SLAVE",
//       "localhost_12001" : "SLAVE",
//       "localhost_12002" : "MASTER"
//     },
//     "MyResource_2" : {
//       "localhost_12000" : "MASTER",
//       "localhost_12001" : "SLAVE",
//       "localhost_12002" : "SLAVE"
//     },
//     "MyResource_3" : {
//       "localhost_12000" : "SLAVE",
//       "localhost_12001" : "SLAVE",
//       "localhost_12002" : "MASTER"
//     },
//     "MyResource_4" : {
//       "localhost_12000" : "MASTER",
//       "localhost_12001" : "SLAVE",
//       "localhost_12002" : "SLAVE"
//     },
//     "MyResource_5" : {
//       "localhost_12000" : "SLAVE",
//       "localhost_12001" : "MASTER",
//       "localhost_12002" : "SLAVE"
//     }
//   },
//   "listFields" : {
//     "MyResource_0" : [ "localhost_12001", "localhost_12000", "localhost_12002" ],
//     "MyResource_1" : [ "localhost_12002", "localhost_12000", "localhost_12001" ],
//     "MyResource_2" : [ "localhost_12000", "localhost_12002", "localhost_12001" ],
//     "MyResource_3" : [ "localhost_12002", "localhost_12001", "localhost_12000" ],
//     "MyResource_4" : [ "localhost_12000", "localhost_12001", "localhost_12002" ],
//     "MyResource_5" : [ "localhost_12001", "localhost_12000", "localhost_12002" ]
//   }
// }
type IdealState struct {
	*Record
}

func NewIdealStateFromRecord(record *Record) *IdealState {
	return &IdealState{Record: record}
}

func (is *IdealState) Resource() string {
	return is.ID
}

func (is *IdealState) SetRebalanceMode(mode string) {
	is.SetSimpleField("REBALANCE_MODE", mode)
}

func (is *IdealState) RebalanceMode() string {
	return is.GetStringField("REBALANCE_MODE", "")
}

func (is IdealState) RebalanceStrategy() string {
	return is.GetStringField("REBALANCE_STRATEGY", "")
}

func (is IdealState) PreferenceList(partitionName string) []string {
	return is.GetListField(partitionName)
}

func (is IdealState) ExternalViewDisabled() bool {
	return is.GetBooleanField("EXTERNAL_VIEW_DISABLED", false)
}

func (is IdealState) ResourceGroupName() string {
	return is.GetStringField("RESOURCE_GROUP_NAME", "")
}

func (is *IdealState) EnableGroupRouting(yes bool) {
	is.SetBooleanField("GROUP_ROUTING_ENABLED", yes)
}

func (is *IdealState) SetRebalancerClassName(clazz string) {
	is.SetSimpleField("REBALANCER_CLASS_NAME", clazz)
}

func (is *IdealState) RebalancerClassName() string {
	return is.GetStringField("REBALANCER_CLASS_NAME", "")
}

func (is *IdealState) MaxPartitionsPerInstance() int {
	return is.GetIntField("MAX_PARTITIONS_PER_INSTANCE", int(math.MaxInt64))
}

func (is *IdealState) SetMaxPartitionsPerInstance(max int) {
	is.SetIntField("MAX_PARTITIONS_PER_INSTANCE", max)
}

func (is IdealState) Replicas() string {
	return is.GetStringField("REPLICAS", "")
}

func (is *IdealState) SetReplicas(replicas string) {
	is.SetStringField("REPLICAS", replicas)
}

func (is IdealState) NumPartitions() int {
	n, _ := strconv.Atoi(is.GetStringField("NUM_PARTITIONS", "-1"))
	return n
}

func (is IdealState) SetNumPartitions(n int) {
	is.SetStringField("NUM_PARTITIONS", strconv.Itoa(n))
}

// StateModelDefRef returns the state model associated with this resource.
func (is IdealState) StateModelDefRef() string {
	return is.GetStringField("STATE_MODEL_DEF_REF", "")
}

func (is IdealState) InstanceGroupTag() string {
	return is.GetStringField("INSTANCE_GROUP_TAG", "")
}

func (is *IdealState) SetInstanceGroupTag(tag string) {
	is.SetStringField("INSTANCE_GROUP_TAG", tag)
}

func (is IdealState) Valid() bool {
	isInvalid := is.NumPartitions() < 0 ||
		is.StateModelDefRef() == ""
	if isInvalid {
		return false
	}

	if is.RebalanceMode() == "SEMI_AUTO" {
		if is.Replicas() == "" {
			return false
		}
	}

	return true
}
