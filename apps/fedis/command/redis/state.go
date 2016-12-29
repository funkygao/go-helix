package redis

import (
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
)

func (r *redisParticipant) StateModel() *helix.StateModel {
	// register state model before connecting
	sm := helix.NewStateModel()
	must(sm.AddTransitions([]helix.Transition{
		{"MASTER", "SLAVE", r.master2slave},
		{"SLAVE", "MASTER", r.slave2master},
		{"OFFLINE", "SLAVE", r.offline2slave},
		{"SLAVE", "OFFLINE", r.slave2offline},
	}))

	return sm
}

func (r *redisParticipant) master2slave(message *model.Message, ctx *helix.Context) {
	log.Info(color.Cyan("resource[%s/%s] %s->%s", message.Resource(),
		message.PartitionName(), message.FromState(), message.ToState()))
}

func (r *redisParticipant) slave2master(message *model.Message, ctx *helix.Context) {
	log.Info(color.Green("resource[%s/%s] %s->%s", message.Resource(),
		message.PartitionName(), message.FromState(), message.ToState()))

	// catch up previous master, enable writes, etc.
}

func (r *redisParticipant) offline2slave(message *model.Message, ctx *helix.Context) {
	log.Info(color.Blue("resource[%s/%s] %s->%s", message.Resource(),
		message.PartitionName(), message.FromState(), message.ToState()))

	// bootstrap data, setup replication, etc.
}

func (r *redisParticipant) slave2offline(message *model.Message, ctx *helix.Context) {
	log.Info(color.Red("resource[%s/%s] %s->%s", message.Resource(),
		message.PartitionName(), message.FromState(), message.ToState()))
}
