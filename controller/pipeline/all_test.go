package pipeline

import (
	"testing"

	"github.com/funkygao/assert"
)

type dummyStage struct {
}

func (ds dummyStage) Init()                    {}
func (ds dummyStage) Name() string             { return "dummy" }
func (ds dummyStage) PreProcess()              {}
func (ds dummyStage) Process(evt string) error { return nil }
func (ds dummyStage) PostProcess()             {}
func (ds dummyStage) Release()                 {}

func newDummyStage() Stage {
	return &dummyStage{}
}

func TestPiplelineBasic(t *testing.T) {
	p := NewPipeline()
	p.AddStage(newDummyStage())
	assert.Equal(t, 1, len(p.stages))
	p.HandleEvent("evt")
}

func TestPipelineRegistry(t *testing.T) {
	r := NewPipelineRegistry()
	p := NewPipeline()
	p.AddStage(newDummyStage())

	r.Register("eventName", p)
	ps := r.PipelineForEvent("eventName")
	assert.Equal(t, 1, len(ps))
	assert.Equal(t, p, ps[0])
	for _, pl := range ps {
		t.Logf("%+v %+v", pl, pl.stages)
		pl.HandleEvent("eventName")
	}

}
