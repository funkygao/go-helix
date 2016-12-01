package helix

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/funkygao/golib/pipestream"
)

// AddTestCluster calls helix-admin.sh --zkSvr localhost:2181 --addCluster
func AddTestCluster(cluster string) error {
	cmd := "/opt/helix/bin/helix-admin.sh --zkSvr localhost:2181 --addCluster " + strings.TrimSpace(cluster)
	if _, err := RunCommand(cmd); err != nil {
		return err
	}
	return nil
}

// DropTestCluster /opt/helix/bin/helix-admin.sh --zkSvr localhost:2181 --dropCluster
func DropTestCluster(cluster string) error {
	cmd := "/opt/helix/bin/helix-admin.sh --zkSvr localhost:2181 --dropCluster " + strings.TrimSpace(cluster)
	if _, err := RunCommand(cmd); err != nil {
		return err
	}
	return nil
}

// AddNode /opt/helix/bin/helix-admin.sh --zkSvr localhost:2181  --addNode
func AddNode(cluster string, host string, port string) error {
	cmd := "/opt/helix/bin/helix-admin.sh"
	if _, err := execCommand(cmd,
		"--zkSvr", "localhost:2181", "--addNode", fmt.Sprintf("%s:%s", host, port)); err != nil {
		return err
	}
	return nil
}

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

// StartController
func StartController(cluster string) error {
	cmd := "/opt/helix/bin/run-helix-controller.sh"
	if _, err := execCommand(cmd,
		"--zkSvr", "localhost:2181", "--cluster", cluster); err != nil {
		return err
	}
	return nil
}

// StopController sudo /usr/bin/supervisorctl stop helixcontroller
func StopController() error {
	if _, err := RunCommand("sudo /usr/bin/supervisorctl stop helixcontroller"); err != nil {
		return err
	}
	return nil
}

// StartParticipant /usr/bin/supervisorctl start participant_
func StartParticipant(port string) error {
	command := "/usr/bin/supervisorctl start participant_" + port
	if _, err := RunCommand(command); err != nil {
		return err
	}

	return nil
}

// StopParticipant /usr/bin/supervisorctl stop participant_
func StopParticipant(port string) error {
	command := "/usr/bin/supervisorctl stop participant_" + port
	if _, err := RunCommand(command); err != nil {
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

// RunCommand execute command via ssh
func RunCommand(command string) (string, error) {
	return "", nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func strSliceContains(a []string, s string) bool {
	for _, ele := range a {
		if ele == s {
			return true
		}
	}

	return false
}
