package model

// ideal state content example:
// {
//   "id" : "MyResource",
//   "simpleFields" : {
//     "IDEAL_STATE_MODE" : "AUTO",
//     "NUM_PARTITIONS" : "6",
//     "REBALANCE_MODE" : "SEMI_AUTO",
//     "REBALANCE_STRATEGY" : "DEFAULT",
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
