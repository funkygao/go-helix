package zk

import (
	"sync"

	"github.com/funkygao/go-helix"
)

var _ helix.StateMachineEngine = &stateMachineEngine{}

type stateMachineEngine struct {
	sync.RWMutex
	m *Manager

	// all registered state model callbacks
	stateModels map[string]*helix.StateModel
}

func newStateMachineEngine(m *Manager) *stateMachineEngine {
	return &stateMachineEngine{
		m:           m,
		stateModels: make(map[string]*helix.StateModel),
	}
}

func (sme *stateMachineEngine) RegisterStateModel(stateModel string, sm *helix.StateModel) error {
	sme.Lock()
	defer sme.Unlock()

	if _, present := sme.stateModels[stateModel]; present {
		return helix.ErrDupStateModelName
	}

	sme.stateModels[stateModel] = sm
	return nil
}

func (sme *stateMachineEngine) RemoveStateModel(stateModel string) error {
	sme.Lock()
	defer sme.Unlock()

	delete(sme.stateModels, stateModel)
	return nil
}

func (sme *stateMachineEngine) StateModel(stateModel string) (*helix.StateModel, bool) {
	sme.RLock()
	defer sme.RUnlock()

	r, present := sme.stateModels[stateModel]
	return r, present
}
