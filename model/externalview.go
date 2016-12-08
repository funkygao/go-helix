package model

type ExternalView struct {
	*Record
}

func NewExternalView(r *Record) *ExternalView {
	return &ExternalView{Record: r}
}
