package model

type StateModelDef struct {
	*Record
}

func NewStateModelDef(stateModel string) *StateModelDef {
	return &StateModelDef{Record: NewRecord(stateModel)}
}

func (smd *StateModelDef) AddState(state string, priority int) *StateModelDef {
	return smd
}

func (smd *StateModelDef) AddTransition(fromState, toState string) *StateModelDef {
	return smd
}

func (smd *StateModelDef) SetInitialState(state string) *StateModelDef {
	smd.SetStringField("INITIAL_STATE", state)
	return smd
}

func (smd *StateModelDef) SetStaticUpperBound(state string, bound int) *StateModelDef {
	return smd
}

func (smd *StateModelDef) SetDynamicUpperBound(state string, bound string) *StateModelDef {
	return smd
}