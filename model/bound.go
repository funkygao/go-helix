package model

type DynamicBound string

const (
	// Refers to the number of replicas specified during resource creation.
	// This allows having different replication factor for each resource without having to create a different state machine.
	DynamicBoundR = "R"

	// Refers to all nodes in the cluster. Useful for resources that need
	// to exist on all nodes. This way one can add/remove nodes without having the change the bounds.
	DynamicBoundN = "N"
)
