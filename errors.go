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
	ErrClusterNotSetup = errors.New("cluster not setup")

	// ErrNodeAlreadyExists the zookeeper node exists when it is not expected to
	ErrNodeAlreadyExists = errors.New("node already exists in cluster")

	// ErrNodeNotExist the zookeeper node does not exist when it is expected to
	ErrNodeNotExist = errors.New("node does not exist in config for cluster")

	// ErrInstanceNotExist the instance of a cluster does not exist when it is expected to
	ErrInstanceNotExist = errors.New("node does not exist in instances for cluster")

	// ErrStateModelDefNotExist the state model definition is expected to exist in zookeeper
	ErrStateModelDefNotExist = errors.New("state model not exist in cluster")

	// ErrResourceExists the resource already exists in cluster and cannot be added again
	ErrResourceExists = errors.New("resource already exists in cluster")

	// ErrResourceNotExists the resource does not exists and cannot be removed
	ErrResourceNotExists = errors.New("resource not exists in cluster")
)

var (
	// ErrEnsureParticipantConfig is returned when participant configuration cannot be
	// created in zookeeper
	ErrEnsureParticipantConfig = errors.New("Participant configuration could not be added")

	// ErrInvalidAddResourceOption is returned when user provides a invalid resource to add.
	ErrInvalidAddResourceOption = errors.New("Invalid AddResourceOption")

	ErrEmptyStateModel = errors.New("Register at least one valid state model before connecting.")

	// ErrNotConnected is returned when call a function without calling Connect() beforehand.
	ErrNotConnected = errors.New("Not connected yet")

	ErrPartialSuccess = errors.New("Partial success")
)
