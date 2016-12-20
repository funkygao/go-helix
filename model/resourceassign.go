package model

// ResourceAssignment represents the assignments of replicas for an entire resource, keyed on
// partitions of the resource.
type ResourceAssignment struct {
	*Record
}

func (ra *ResourceAssignment) Resource() string {
	return ra.ID
}

// MappedPartitions returns the currently mapped partition names.
func (ra *ResourceAssignment) MappedPartitions() []string {
	return nil
}

// ReplicaMap returns the instance, state pairs for a partition.
// e,g. {"localhost_10001": "MASTER"}
func (ra *ResourceAssignment) ReplicaMap(partition string) map[string]string {
	return nil
}
