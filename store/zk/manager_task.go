package zk

import (
	log "github.com/funkygao/log4go"
)

func (m *Manager) startTimerTasks() []error {
	var errs []error
	for _, t := range m.timerTasks {
		if err := t.Start(); err != nil {
			errs = append(errs, err)
		}
	}

	log.Debug("%s start timer tasks with %+v", m.shortID(), errs)

	return errs
}

func (m *Manager) stopTimerTasks() {
	log.Debug("%s stop timer tasks", m.shortID())
	for _, t := range m.timerTasks {
		t.Stop()
	}
}
