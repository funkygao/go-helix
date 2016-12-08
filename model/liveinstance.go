package model

type LiveInstance struct {
	*Record
}

func NewLiveInstance(r *Record) *LiveInstance {
	return &LiveInstance{Record: r}
}
