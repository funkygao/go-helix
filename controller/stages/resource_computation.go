package stages

import (
	"github.com/funkygao/go-helix/controller/pipeline"
)

var _ pipeline.Stage = &ResourceComputationStage{}

type ResourceComputationStage struct {
}

func (r *ResourceComputationStage) Init() {

}

func (r *ResourceComputationStage) Name() string {
	return "ResourceComputationStage"
}

func (r *ResourceComputationStage) PreProcess() {}

func (r *ResourceComputationStage) PostProcess() {}

func (r *ResourceComputationStage) Process(evt string) error {
	return nil
}

func (r *ResourceComputationStage) Release() {}
