package model

type LiveInstance struct {
	*Record
}

func NewLiveInstanceFromRecord(r *Record) *LiveInstance {
	return &LiveInstance{Record: r}
}
