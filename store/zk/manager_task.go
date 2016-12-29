package zk

import (
	log "github.com/funkygao/log4go"
)

func (m *Manager) startTimerTasks() {
	log.Trace("%s start timer tasks", m.shortID())

	m.Lock()
	defer m.Unlock()

	for _, t := range m.timerTasks {
		go t.Start()
	}

}

func (m *Manager) stopTimerTasks() {
	log.Trace("%s stop timer tasks", m.shortID())

	m.Lock()
	defer m.Unlock()

	for _, t := range m.timerTasks {
		t.Stop()
	}
}
