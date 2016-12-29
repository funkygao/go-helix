package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	golog "log"
	"strings"

	"github.com/funkygao/go-helix/apps/dfs/command/start"
	"github.com/funkygao/gocli"
)

type Start struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Start) Run(args []string) (exitCode int) {
	var (
		node string
		log  string
	)
	cmdFlags := flag.NewFlagSet("start", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&node, "node", "", "")
	cmdFlags.StringVar(&log, "log", "debug", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	tuples := strings.Split(node, "_")
	if len(tuples) != 2 {
		this.Ui.Output(this.Help())
		return 2
	}

	golog.SetOutput(ioutil.Discard)
	setupLogging(log)

	host := tuples[0]
	port := tuples[1]

	r := start.NewNode(zkSvr, cluster, resource, stateModel, replicas, host, port)
	r.Start()

	return
}

func (*Start) Synopsis() string {
	return "Start a DFS blob store node"
}

func (this *Start) Help() string {
	help := fmt.Sprintf(`
Usage: %s start [options]

    %s

Options:

    -node host_port

    -log debug|info|trace
      Default debug.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
