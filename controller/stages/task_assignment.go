package stages

import (
	"github.com/funkygao/go-helix/controller/pipeline"
)

var _ pipeline.Stage = &TaskAssignmentStage{}

type TaskAssignmentStage struct {
}

func (r *TaskAssignmentStage) Init() {

}

func (r *TaskAssignmentStage) Name() string {
	return "TaskAssignmentStage"
}

func (r *TaskAssignmentStage) PreProcess() {}

func (r *TaskAssignmentStage) PostProcess() {}

func (r *TaskAssignmentStage) Process(evt string) error {
	return nil
}

func (r *TaskAssignmentStage) Release() {}
