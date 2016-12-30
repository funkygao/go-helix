package model

type ExternalView struct {
	*Record
}

func NewExternalViewFromRecord(r *Record) *ExternalView {
	return &ExternalView{Record: r}
}

func (e *ExternalView) Resource() string {
	return e.ID
}

func (e *ExternalView) IdealStateMode() string {
	return e.GetStringField("IDEAL_STATE_MODE", "")
}

func (e *ExternalView) Replicas() int {
	return e.GetIntField("REPLICAS", 0)
}

func (e *ExternalView) NumPartitions() int {
	return e.GetIntField("NUM_PARTITIONS", 0)
}

func (e *ExternalView) InstanceState(instance, partitionName string) string {
	return e.MapFields[partitionName][instance]
}

func (e *ExternalView) InstanceWithState(partitionName, state string) string {
	for instance, s := range e.MapFields[partitionName] {
		if s == state {
			return instance
		}
	}

	return ""
}
