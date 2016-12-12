package pipeline

import (
	"sync"
)

type PipelineRegistry struct {
	sync.RWMutex
	registryMap map[string][]*Pipeline
}

func NewPipelineRegistry() *PipelineRegistry {
	return &PipelineRegistry{
		registryMap: map[string][]*Pipeline{},
	}
}

func (r *PipelineRegistry) Register(eventName string, ps ...*Pipeline) {
	r.Lock()
	defer r.Unlock()

	if _, present := r.registryMap[eventName]; !present {
		r.registryMap[eventName] = make([]*Pipeline, 0)
	}
	for _, p := range ps {
		r.registryMap[eventName] = append(r.registryMap[eventName], p)
	}
}

func (r *PipelineRegistry) PipelineForEvent(eventName string) []*Pipeline {
	r.RLock()
	defer r.RUnlock()
	return r.registryMap[eventName]
}
