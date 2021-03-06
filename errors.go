package helix

import (
	"errors"
)

var (
	ErrNotImplemented = errors.New("Not implemented")
)

var (
	// ErrClusterNotSetup means the helix data structure in zookeeper /{CLUSTER_NAME}
	// is not correct or does not exist
	ErrClusterNotSetup = errors.New("Cluster not setup")

	// ErrNodeAlreadyExists the zookeeper node exists when it is not expected to
	ErrNodeAlreadyExists = errors.New("Node already exists in cluster")

	// ErrNodeNotExist the zookeeper node does not exist when it is expected to
	ErrNodeNotExist = errors.New("Node does not exist in config for cluster")

	// ErrInstanceNotExist the instance of a cluster does not exist when it is expected to
	ErrInstanceNotExist = errors.New("Node does not exist in instances for cluster")

	// ErrStateModelDefNotExist the state model definition is expected to exist in zookeeper
	ErrStateModelDefNotExist = errors.New("State model not exist in cluster")

	// ErrResourceExists the resource already exists in cluster and cannot be added again
	ErrResourceExists = errors.New("Resource already exists in cluster")

	// ErrResourceNotExists the resource does not exists and cannot be removed
	ErrResourceNotExists = errors.New("Resource not exists in cluster")
)

var (
	// ErrEnsureParticipantConfig is returned when participant configuration cannot be
	// created in zookeeper
	ErrEnsureParticipantConfig = errors.New("Participant configuration could not be added")

	// ErrInvalidAddResourceOption is returned when user provides a invalid resource to add.
	ErrInvalidAddResourceOption = errors.New("Invalid AddResourceOption")

	ErrInvalidClusterName = errors.New("Invalid cluster name")

	ErrEmptyStateModel = errors.New("Register at least one valid state model before connecting.")

	// ErrNotConnected is returned when call a function without calling Connect() beforehand.
	ErrNotConnected = errors.New("Not connected yet")

	ErrPartialSuccess = errors.New("Partial success")

	ErrDupStateModelName = errors.New("Register a state model over once")

	ErrSessionChanged = errors.New("SessionID changed")

	ErrDupStateTransition = errors.New("Register a state model transition over once")

	ErrNotEmpty = errors.New("Not empty")

	ErrInvalidArgument = errors.New("Invalid arguments")

	ErrInvalidMessage = errors.New("Invalid message")

	ErrDupOperation = errors.New("Duplicated operation")

	ErrUnkownMessageType = errors.New("Unknown message type")

	ErrSystem = errors.New("Unknown system error")
)
