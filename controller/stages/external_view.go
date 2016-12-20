package stages

import (
	"github.com/funkygao/go-helix/controller/pipeline"
)

var _ pipeline.Stage = &ExternalViewComputeStage{}

type ExternalViewComputeStage struct {
}

func (r *ExternalViewComputeStage) Init() {

}

func (r *ExternalViewComputeStage) Name() string {
	return "ExternalViewComputeStage"
}

func (r *ExternalViewComputeStage) PreProcess() {}

func (r *ExternalViewComputeStage) PostProcess() {}

func (r *ExternalViewComputeStage) Process(evt string) error {
	return nil
}

func (r *ExternalViewComputeStage) Release() {}
