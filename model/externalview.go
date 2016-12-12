package model

type ExternalView struct {
	*Record
}

func NewExternalViewFromRecord(r *Record) *ExternalView {
	return &ExternalView{Record: r}
}
