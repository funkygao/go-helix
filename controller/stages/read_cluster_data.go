package stages

import (
	"github.com/funkygao/go-helix/controller/pipeline"
)

var _ pipeline.Stage = &ReadClusterDataStage{}

type ReadClusterDataStage struct {
}

func (r *ReadClusterDataStage) Init() {

}

func (r *ReadClusterDataStage) Name() string {
	return "ReadClusterDataStage"
}

func (r *ReadClusterDataStage) PreProcess() {}

func (r *ReadClusterDataStage) PostProcess() {}

func (r *ReadClusterDataStage) Process(evt string) error {
	return nil
}

func (r *ReadClusterDataStage) Release() {}
