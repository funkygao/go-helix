package builder

import (
	"github.com/funkygao/go-helix/model"
)

type idealStateBuilder struct {
	resource string
	record   *model.Record
}

func NewIdealStateBuilder(resource string) *idealStateBuilder {
	return &idealStateBuilder{
		resource: resource,
		record:   model.NewRecord(resource),
	}
}

func (is idealStateBuilder) Build() *model.IdealState {
	r := model.NewIdealStateFromRecord(is.record)
	if !r.Valid() {

	}
	return r
}
