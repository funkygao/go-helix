package zk

import (
	log "github.com/funkygao/log4go"
)

func (m *Manager) startTimerTasks() []error {
	log.Trace("%s start timer tasks", m.shortID())

	m.Lock()
	defer m.Unlock()

	var errs []error
	for _, t := range m.timerTasks {
		if err := t.Start(); err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

func (m *Manager) stopTimerTasks() {
	log.Trace("%s stop timer tasks", m.shortID())

	m.Lock()
	defer m.Unlock()

	for _, t := range m.timerTasks {
		t.Stop()
	}
}
