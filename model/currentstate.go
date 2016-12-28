package model

type CurrentState struct {
	*Record
}

func NewCurrentStateFromRecord(r *Record) *CurrentState {
	return &CurrentState{Record: r}
}

func (c *CurrentState) SetCurrentState(state string) {
	c.SetStringField("CURRENT_STATE", state)
}
