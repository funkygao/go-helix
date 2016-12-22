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
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/debug"
	"github.com/funkygao/golib/sync2"
	glog "github.com/funkygao/log4go"
)

type Trace struct {
	Ui  cli.Ui
	Cmd string

	eventSeq  sync2.AtomicInt64
	spectator helix.HelixManager

	lock             sync.Mutex
	liveInstances    map[string]bool
	controllerLeader string
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

	golog.SetOutput(ioutil.Discard) // disable zk low level logging
	setupLogging(log)
	if silent {
		glog.Disable()
	}

	this.Ui.Infof("tracing cluster %s", cluster)

	spectator, err := zk.NewZkSpectator(cluster, "", "", zkSvr)
	must(err)
	must(spectator.Connect())

	this.spectator = spectator

	this.Ui.Warn("1. tracing controller leader changes...")
	must(spectator.AddControllerListener(this.controller))

	this.Ui.Warn("2. tracing external view changes...")
	must(spectator.AddExternalViewChangeListener(this.external))

	this.Ui.Warn("3. tracing ideal state changes...")
	must(spectator.AddIdealStateChangeListener(this.ideal))

	this.Ui.Warn("4. tracing live instance changes...")
	must(spectator.AddLiveInstanceChangeListener(this.live))

	this.Ui.Warn("5. tracing instance config changes...")
	must(spectator.AddInstanceConfigChangeListener(this.instanceConfig))

	this.Ui.Warn("6. tracing leader messages...")
	must(spectator.AddControllerMessageListener(this.controllerMsg))

	this.Ui.Info("waiting Ctrl-C...")

	// block until SIGINT and SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt)
	<-c

	this.Ui.Info("disconnecting...")
	spectator.Disconnect()
	glog.Close()
	this.Ui.Info("bye!")

	return
}

func (this *Trace) live(liveInstances []*model.LiveInstance, ctx *helix.Context) {
	seq := this.eventSeq.Add(1)
	this.Ui.Errorf("[%d] liveInstances %+v", seq, liveInstances)

	this.Ui.Output(color.Cyan("[%d] %+v", seq, debug.Callstack(4)))

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

func (this *Trace) external(externalViews []*model.ExternalView, ctx *helix.Context) {
	seq := this.eventSeq.Add(1)
	this.Ui.Errorf("[%d] externalViews %+v", seq, externalViews)
	this.Ui.Output(color.Cyan("[%d] %+v", seq, debug.Callstack(4)))
}

func (this *Trace) ideal(idealState []*model.IdealState, ctx *helix.Context) {
	seq := this.eventSeq.Add(1)
	this.Ui.Errorf("[%d] idealState %+v", seq, idealState)
	this.Ui.Output(color.Cyan("[%d] %+v", seq, debug.Callstack(4)))
}

func (this *Trace) controller(ctx *helix.Context) {
	seq := this.eventSeq.Add(1)

	leader := this.spectator.ClusterManagementTool().ControllerLeader(cluster)
	this.controllerLeader = leader
	this.Ui.Errorf("[%d] controller leader -> %s", seq, leader)
	this.Ui.Output(color.Cyan("[%d] %+v", seq, debug.Callstack(4)))
}

func (this *Trace) controllerMsg(instance string, messages []*model.Message, ctx *helix.Context) {
	seq := this.eventSeq.Add(1)
	this.Ui.Errorf("[%d] controller msg [%s] %+v", seq, instance, messages)
	this.Ui.Output(color.Cyan("[%d] %+v", seq, debug.Callstack(4)))
}

func (this *Trace) messages(instance string, messages []*model.Message, ctx *helix.Context) {
	seq := this.eventSeq.Add(1)
	this.Ui.Errorf("[%d] participant msg [%s] %+v", seq, instance, messages)
	this.Ui.Output(color.Cyan("[%d] %+v", seq, debug.Callstack(4)))
}

func (this *Trace) instanceConfig(configs []*model.InstanceConfig, ctx *helix.Context) {
	seq := this.eventSeq.Add(1)
	this.Ui.Errorf("[%d]instance config  %+v", seq, configs)
	this.Ui.Output(color.Cyan("[%d] %+v", seq, debug.Callstack(4)))
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
