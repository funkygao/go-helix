package healthcheck

import (
	"github.com/funkygao/go-helix"
)

type ParticipanthealthcheckTask struct {
}

func NewParticipanthealthcheckTask() helix.HelixTimerTask {
	return &ParticipanthealthcheckTask{}
}

func (p *ParticipanthealthcheckTask) Start() error {
	return nil
}

func (p *ParticipanthealthcheckTask) Stop() {

}
