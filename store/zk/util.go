package zk

import (
	"os/exec"
)

func any(errors ...error) error {
	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

func execCommand(name string, args ...string) (err error, exitCh chan error) {
	exitCh = make(chan error)
	cmd := exec.Command(name, args...)
	waitStart := make(chan struct{})
	go func() {
		<-waitStart
		if err := cmd.Wait(); err != nil {
			exitCh <- err
		} else {
			close(exitCh)
		}
	}()

	err = cmd.Start()
	close(waitStart)
	return
}
