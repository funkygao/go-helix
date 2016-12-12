package model

type CurrentState struct {
	*Record
}

func NewCurrentStateFromRecord(r *Record) *CurrentState {
	return &CurrentState{Record: r}
}
