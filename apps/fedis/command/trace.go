package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	golog "log"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/go-helix/store/zk"
	"github.com/funkygao/gocli"
	glog "github.com/funkygao/log4go"
)

type Trace struct {
	Ui  cli.Ui
	Cmd string

	spectator     helix.HelixManager
	lock          sync.Mutex
	liveInstances map[string]bool
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

	this.liveInstances = make(map[string]bool)

	setupLogging(log)
	if silent {
		golog.SetOutput(ioutil.Discard)
		glog.Disable()
	}

	this.Ui.Infof("tracing cluster %s", cluster)

	spectator, err := zk.NewZkSpectator(cluster, "", "", zkSvr)
	must(err)
	must(spectator.Connect())

	this.spectator = spectator

	this.Ui.Warn("tracing controller leader changes...")
	if err = spectator.AddControllerListener(this.controller); err != nil {
		this.Ui.Error(err.Error())
	}

	this.Ui.Warn("tracing external view changes...")
	if err = spectator.AddExternalViewChangeListener(this.external); err != nil {
		this.Ui.Error(err.Error())
	}

	this.Ui.Warn("tracing ideal state changes...")
	if err = spectator.AddIdealStateChangeListener(this.ideal); err != nil {
		this.Ui.Error(err.Error())
	}

	this.Ui.Warn("tracing live instance changes...")
	if err = spectator.AddLiveInstanceChangeListener(this.live); err != nil {
		this.Ui.Error(err.Error())
	}

	this.Ui.Info("waiting Ctrl-C...")

	// block until SIGINT and SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt)
	<-c

	this.Ui.Info("disconnecting...")
	spectator.Disconnect()
	this.Ui.Info("bye!")

	return
}

func (this *Trace) external(externalViews []*model.ExternalView, context *helix.Context) {
	this.Ui.Outputf("externalViews %+v", externalViews)
}

func (this *Trace) ideal(idealState []*model.IdealState, context *helix.Context) {
	this.Ui.Outputf("idealState %+v", idealState)
}

func (this *Trace) live(liveInstances []*model.LiveInstance, context *helix.Context) {
	this.Ui.Outputf("liveInstances %+v", liveInstances)

	this.lock.Lock()
	defer this.lock.Unlock()

	for _, live := range liveInstances {
		if ok, present := this.liveInstances[live.Node()]; !present || !ok {
			this.Ui.Warnf("tracing %s messages...", live.Node())
			this.spectator.AddMessageListener(live.Node(), this.messages)
		}
	}

	for k := range this.liveInstances {
		this.liveInstances[k] = false
	}
	for _, live := range liveInstances {
		this.liveInstances[live.Node()] = true
	}
	for _, ok := range this.liveInstances {
		if !ok {
			//this.spectator.RemoveListener(path, lisenter) TODO
		}
	}
}

func (this *Trace) controller(ctx *helix.Context) {
	leader := this.spectator.ClusterManagementTool().ControllerLeader(cluster)
	this.Ui.Outputf("controller leader -> %s", leader)
}

func (this *Trace) messages(instance string, messages []*model.Message, context *helix.Context) {
	this.Ui.Outputf("[%s] %+v", instance, messages)
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
