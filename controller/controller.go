// Package controller provides implementation of the default Helix controller.
package controller

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/controller/pipeline"
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
	go c.processClusterEvents()
	return c
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
