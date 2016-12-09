package model

import (
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

func (is *IdealState) SetRebalancerClassName(clazz string) {
	is.SetSimpleField("REBALANCER_CLASS_NAME", clazz)
}

func (is *IdealState) RebalancerClassName() string {
	return is.GetStringField("REBALANCER_CLASS_NAME", "")
}

func (is *IdealState) MaxPartitionsPerInstance() int {
	return is.GetIntField("MAX_PARTITIONS_PER_INSTANCE", 0)
}

func (is *IdealState) SetMaxPartitionsPerInstance(max int) {
	is.SetIntField("MAX_PARTITIONS_PER_INSTANCE", max)
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
