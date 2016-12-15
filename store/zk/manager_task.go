package zk

func (m *Manager) StartTimerTasks() []error {
	var errs []error
	for _, t := range m.timerTasks {
		if err := t.Start(); err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

func (m *Manager) StopTimerTasks() {
	for _, t := range m.timerTasks {
		t.Stop()
	}
}
