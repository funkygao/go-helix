// Package controller provides implementation of the default Helix controller.
package controller

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/controller/pipeline"
	"github.com/funkygao/go-helix/controller/stages"
	"github.com/funkygao/golib/sync2"
	log "github.com/funkygao/log4go"
)

// GenericHelixController is a loop that drives the current state towards the ideal state.
type GenericHelixController struct {
	manager  helix.HelixManager
	paused   sync2.AtomicBool
	registry *pipeline.PipelineRegistry
}

func NewGenericHelixController() *GenericHelixController {
	c := &GenericHelixController{}
	c.createDefaultRegistry()
	go c.processClusterEvents()
	return c
}

func (c *GenericHelixController) createDefaultRegistry() {
	c.registry = pipeline.NewPipelineRegistry()

	// cluster data cache refresh
	dataRefresh := pipeline.NewPipeline()
	dataRefresh.AddStage(&stages.ReadClusterDataStage{})

	// rebalance pipeline
	rebalancePipeline := pipeline.NewPipeline()
	rebalancePipeline.AddStage(&stages.ResourceComputationStage{})
	rebalancePipeline.AddStage(&stages.TaskAssignmentStage{})

	// external view generation
	externalViewPipeline := pipeline.NewPipeline()
	externalViewPipeline.AddStage(&stages.ExternalViewComputeStage{})

	c.registry.Register("idealStateChange", dataRefresh, rebalancePipeline)
	c.registry.Register("currentStateChange", dataRefresh, rebalancePipeline, externalViewPipeline)
	c.registry.Register("configChange", dataRefresh, rebalancePipeline)

}

func (c *GenericHelixController) processClusterEvents() {

}

func (c *GenericHelixController) handleEvent(evt string) {
	if !c.manager.IsLeader() {
		log.Warn("%s is not leader, ignore the event", c.manager.Instance())
		return
	}

	if c.paused.Get() {
		log.Warn("%s is paused, ignore the event", c.manager.Instance())
		return
	}

	var err error
	for _, p := range c.registry.PipelineForEvent(evt) {
		if err = p.HandleEvent(evt); err != nil {
			// TODO
			log.Error("%s %v", c.manager.Instance(), err)
		}
		p.Finish()
	}

}
