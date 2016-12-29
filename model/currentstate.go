package model

type CurrentState struct {
	*Record
}

func NewCurrentStateFromRecord(r *Record) *CurrentState {
	return &CurrentState{Record: r}
}

func (c *CurrentState) SetCurrentState(partitionName, state string) {
	// FIXME if !present in map
	c.MapFields[partitionName]["CURRENT_STATE"] = state
}
