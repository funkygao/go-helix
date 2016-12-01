package helix

import (
	"bufio"

	"github.com/funkygao/golib/pipestream"
)

// AddResource /opt/helix/bin/helix-admin.sh --zkSvr localhost:2181 --addResource
func AddResource(cluster string, resource string, replica string) error {
	cmd := "/opt/helix/bin/helix-admin.sh"
	if _, err := execCommand(cmd,
		"--zkSvr", "localhost:2181", "--addResource", cluster, resource, replica); err != nil {
		return err
	}
	return nil
}

// Rebalance /opt/helix/bin/helix-admin.sh --zkSvr localhost:2181 --rebalance
func Rebalance(cluster string, resource string, replica string) error {
	cmd := "/opt/helix/bin/helix-admin.sh"
	if _, err := execCommand(cmd,
		"--zkSvr", "localhost:2181", "--rebalance", cluster, resource, replica); err != nil {
		return err
	}
	return nil
}

// StartController will start controller on localhost.
func StartController(cluster string) error {
	cmd := "/opt/helix/bin/run-helix-controller.sh"
	if _, err := execCommand(cmd,
		"--zkSvr", "localhost:2181", "--cluster", cluster); err != nil {
		return err
	}
	return nil
}

func execCommand(command string, args ...string) (string, error) {
	cmd := pipestream.New(command, args...)
	err := cmd.Open()
	if err != nil {
		return "", err
	}
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)

	output := make([]string, 0)
	for scanner.Scan() {
		output = append(output, scanner.Text())
	}
	if scanner.Err() != nil {
		return "", scanner.Err()
	}
	return "", nil
}
