package model

type CurrentState struct {
	*Record
}

func NewCurrentState(r *Record) *CurrentState {
	return &CurrentState{Record: r}
}
