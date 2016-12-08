package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/go-helix/apps/fedis/command/redis"
	"github.com/funkygao/gocli"
	log "github.com/funkygao/log4go"
)

type Redis struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Redis) Run(args []string) (exitCode int) {
	var (
		node string
		log  string
	)
	cmdFlags := flag.NewFlagSet("redis", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&node, "node", "", "")
	cmdFlags.StringVar(&log, "log", "debug", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	tuples := strings.Split(node, "_")
	if len(tuples) != 2 {
		this.Ui.Error("-node must in form of host_port")
		return 2
	}

	this.setupLogging(log)

	host := tuples[0]
	port := tuples[1]

	r := redis.NewNode(zkSvr, cluster, resource, stateModel, replicas, host, port)
	r.Start()

	return
}

func (*Redis) setupLogging(level string) {
	logLevel := log.ToLogLevel(level)
	for _, filter := range log.Global {
		filter.Level = logLevel
	}
}

func (*Redis) Synopsis() string {
	return "Start a redis instance"
}

func (this *Redis) Help() string {
	help := fmt.Sprintf(`
Usage: %s redis [options]

    %s

Options:

    -node host_port

    -kill host_port

    -log debug|info|trace
      Default debug.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
