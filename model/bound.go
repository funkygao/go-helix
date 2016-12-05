package model

type DynamicBound string

const (
	// The number of replicas in the state is at most the specified replica count for the partition.
	DynamicBoundR = "R"

	// The number of replicas in the state is at most the number of live participants in the cluster.
	DynamicBoundN = "N"
)
