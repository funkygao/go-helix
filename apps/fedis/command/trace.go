package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/go-helix/store/zk"
	"github.com/funkygao/gocli"
	glog "github.com/funkygao/log4go"
)

type Trace struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Trace) Run(args []string) (exitCode int) {
	var (
		log    string
		silent bool
	)
	cmdFlags := flag.NewFlagSet("trace", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&log, "log", "debug", "")
	cmdFlags.BoolVar(&silent, "s", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	setupLogging(log)
	if silent {
		glog.Disable()
	}

	this.Ui.Infof("tracing cluster %s", cluster)

	spectator, err := zk.NewZkSpectator(cluster, "", "", zkSvr)
	must(err)
	must(spectator.Connect())

	if err = spectator.AddExternalViewChangeListener(
		func(externalViews []*model.ExternalView, context *helix.Context) {
			this.Ui.Outputf("externalViews %+v", externalViews)
		}); err != nil {
		this.Ui.Error(err.Error())
	}
	if err = spectator.AddIdealStateChangeListener(
		func(idealState []*model.IdealState, context *helix.Context) {
			this.Ui.Outputf("idealState %+v", idealState)
		}); err != nil {
		this.Ui.Error(err.Error())
	}
	if err = spectator.AddLiveInstanceChangeListener(
		func(liveInstances []*model.LiveInstance, context *helix.Context) {
			this.Ui.Outputf("liveInstances %+v", liveInstances)
		}); err != nil {
		this.Ui.Error(err.Error())
	}

	select {}

	return
}

func (*Trace) Synopsis() string {
	return "Trace redis cluster change events"
}

func (this *Trace) Help() string {
	help := fmt.Sprintf(`
Usage: %s trace [options]

    %s

    -log debug|info|trace
      Default debug.

    -s
      Silent mode, turn of logging.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
