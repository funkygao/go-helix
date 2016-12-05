package model

type HelixDefinedState string

const (
	// The DROPPED state is used to signify a replica that was served by a given participant, but is no longer served.
	HelixDefinedStateDropped = "DROPPED"

	// The ERROR state is used whenever the participant serving a partition encountered an error and cannot continue to serve the partition.
	HelixDefinedStateError = "ERROR"
)
