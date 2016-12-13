package model

type StateModelDef struct {
	*Record
}

func NewStateModelDefFromRecord(r *Record) *StateModelDef {
	return &StateModelDef{Record: r}
}

func NewStateModelDef(stateModel string) *StateModelDef {
	return &StateModelDef{Record: NewRecord(stateModel)}
}

// TODO
func (smd *StateModelDef) AddState(state string, priority int) *StateModelDef {
	return smd
}

// TODO
func (smd *StateModelDef) AddTransition(fromState, toState string) *StateModelDef {
	return smd
}

func (smd *StateModelDef) SetInitialState(state string) *StateModelDef {
	smd.SetStringField("INITIAL_STATE", state)
	return smd
}

func (smd *StateModelDef) InitialState() string {
	return smd.GetStringField("INITIAL_STATE", "")
}

// TODO
func (smd *StateModelDef) SetStaticUpperBound(state string, bound int) *StateModelDef {
	return smd
}

// TODO
func (smd *StateModelDef) SetDynamicUpperBound(state string, bound DynamicBound) *StateModelDef {
	return smd
}
