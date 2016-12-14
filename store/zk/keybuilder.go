package zk

import (
	"fmt"
)

// keyBuilder generates a Zookeeper path for helix.
//
// /{cluster}/CONFIGS
// /{cluster}/CONFIGS/CLUSTER
// /{cluster}/CONFIGS/CLUSTER/{cluster}
// /{cluster}/CONFIGS/PARTICIPANT
// /{cluster}/CONFIGS/RESOURCE
// /{cluster}/CONTROLLER
// /{cluster}/CONTROLLER/ERRORS
// /{cluster}/CONTROLLER/HISTORY
// /{cluster}/CONTROLLER/LEADER
// /{cluster}/CONTROLLER/MESSAGES
// /{cluster}/CONTROLLER/STATUSUPDATES
// /{cluster}/EXTERNALVIEW
// /{cluster}/IDEALSTATES
// /{cluster}/INSTANCES
// /{cluster}/LIVEINSTANCES
// /{cluster}/PROPERTYSTORE
// /{cluster}/STATEMODELDEFS
// /{cluster}/STATEMODELDEFS/LeaderStandby
// /{cluster}/STATEMODELDEFS/MasterSlave
// /{cluster}/STATEMODELDEFS/OnlineOffline
// /{cluster}/STATEMODELDEFS/STORAGE_DEFAULT_SM_SCHEMATA
// /{cluster}/STATEMODELDEFS/SchedulerTaskQueue
// /{cluster}/STATEMODELDEFS/Task
type keyBuilder struct {
	clusterID string
}

func newKeyBuilder(cluster string) keyBuilder {
	return keyBuilder{clusterID: cluster}
}

func (k *keyBuilder) cluster() string {
	return fmt.Sprintf("/%s", k.clusterID)
}

func (k *keyBuilder) configs() string {
	return fmt.Sprintf("/%s/CONFIGS", k.clusterID)
}

func (k *keyBuilder) clusterConfigs() string {
	return fmt.Sprintf("/%s/CONFIGS/CLUSTER", k.clusterID)
}

func (k *keyBuilder) clusterConfig() string {
	return fmt.Sprintf("/%s/CONFIGS/CLUSTER/%s", k.clusterID, k.clusterID)
}

func (k *keyBuilder) externalView() string {
	return fmt.Sprintf("/%s/EXTERNALVIEW", k.clusterID)
}

func (k *keyBuilder) externalViewForResource(resource string) string {
	return fmt.Sprintf("/%s/EXTERNALVIEW/%s", k.clusterID, resource)
}

func (k *keyBuilder) propertyStore() string {
	return fmt.Sprintf("/%s/PROPERTYSTORE", k.clusterID)
}

func (k *keyBuilder) controller() string {
	return fmt.Sprintf("/%s/CONTROLLER", k.clusterID)
}

func (k *keyBuilder) controllerErrors() string {
	return fmt.Sprintf("/%s/CONTROLLER/ERRORS", k.clusterID)
}

func (k *keyBuilder) controllerHistory() string {
	return fmt.Sprintf("/%s/CONTROLLER/HISTORY", k.clusterID)
}

func (k *keyBuilder) controllerLeader() string {
	return fmt.Sprintf("/%s/CONTROLLER/LEADER", k.clusterID)
}

func (k *keyBuilder) controllerMessages() string {
	return fmt.Sprintf("/%s/CONTROLLER/MESSAGES", k.clusterID)
}

func (k *keyBuilder) controllerMessage(ID string) string {
	return fmt.Sprintf("/%s/CONTROLLER/MESSAGES/%s", k.clusterID, ID)
}

func (k *keyBuilder) pause() string {
	return fmt.Sprintf("/%s/CONTROLLER/PAUSE", k.clusterID)
}

func (k *keyBuilder) controllerStatusUpdates() string {
	return fmt.Sprintf("/%s/CONTROLLER/STATUSUPDATES", k.clusterID)
}

func (k *keyBuilder) idealStates() string {
	return fmt.Sprintf("/%s/IDEALSTATES", k.clusterID)
}

func (k *keyBuilder) idealStateForResource(resource string) string {
	return fmt.Sprintf("/%s/IDEALSTATES/%s", k.clusterID, resource)
}

func (k *keyBuilder) resourceConfigs() string {
	return fmt.Sprintf("/%s/CONFIGS/RESOURCE", k.clusterID)
}

func (k *keyBuilder) resourceConfig(resource string) string {
	return fmt.Sprintf("/%s/CONFIGS/RESOURCE/%s", k.clusterID, resource)
}

func (k *keyBuilder) participantConfigs() string {
	return fmt.Sprintf("/%s/CONFIGS/PARTICIPANT", k.clusterID)
}

func (k *keyBuilder) participantConfig(participantID string) string {
	return fmt.Sprintf("/%s/CONFIGS/PARTICIPANT/%s", k.clusterID, participantID)
}

func (k *keyBuilder) liveInstances() string {
	return fmt.Sprintf("/%s/LIVEINSTANCES", k.clusterID)
}

func (k *keyBuilder) instances() string {
	return fmt.Sprintf("/%s/INSTANCES", k.clusterID)
}

func (k *keyBuilder) instance(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s", k.clusterID, participantID)
}

func (k *keyBuilder) liveInstance(partipantID string) string {
	return fmt.Sprintf("/%s/LIVEINSTANCES/%s", k.clusterID, partipantID)
}

func (k *keyBuilder) currentStates(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/CURRENTSTATES", k.clusterID, participantID)
}

func (k *keyBuilder) currentStatesForSession(participantID string, sessionID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/CURRENTSTATES/%s", k.clusterID, participantID, sessionID)
}

func (k *keyBuilder) currentStateForResource(participantID string, sessionID string, resourceID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/CURRENTSTATES/%s/%s", k.clusterID, participantID, sessionID, resourceID)
}

func (k *keyBuilder) errorsR(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/ERRORS", k.clusterID, participantID)
}

func (k *keyBuilder) errors(participantID string, sessionID string, resourceID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/ERRORS/%s/%s", k.clusterID, participantID, sessionID, resourceID)
}

func (k *keyBuilder) healthReport(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/HEALTHREPORT", k.clusterID, participantID)
}

func (k *keyBuilder) statusUpdates(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/STATUSUPDATES", k.clusterID, participantID)
}

func (k *keyBuilder) stateModelDefs() string {
	return fmt.Sprintf("/%s/STATEMODELDEFS", k.clusterID)
}

func (k *keyBuilder) stateModelDef(stateModel string) string {
	return fmt.Sprintf("/%s/STATEMODELDEFS/%s", k.clusterID, stateModel)
}

func (k *keyBuilder) messages(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/MESSAGES", k.clusterID, participantID)
}

func (k *keyBuilder) message(participantID string, messageID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/MESSAGES/%s", k.clusterID, participantID, messageID)
}

func (k *keyBuilder) constraints() string {
	return "TODO"
}

func (k *keyBuilder) constraint(typ string) string {
	return "TODO"
}
