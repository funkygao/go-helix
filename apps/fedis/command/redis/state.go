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
		{"MASTER", "SLAVE", func(message *model.Message, ctx *helix.Context) {
			log.Info(color.Cyan("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},

		{"SLAVE", "MASTER", func(message *model.Message, ctx *helix.Context) {
			log.Info(color.Green("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))

			// catch up previous master, enable writes, etc.
		}},

		{"OFFLINE", "SLAVE", func(message *model.Message, ctx *helix.Context) {
			log.Info(color.Blue("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))

			// bootstrap data, setup replication, etc.
		}},

		{"SLAVE", "OFFLINE", func(message *model.Message, ctx *helix.Context) {
			log.Info(color.Red("resource[%s/%s] %s->%s", message.Resource(),
				message.PartitionName(), message.FromState(), message.ToState()))
		}},
	}))

	return sm
}

func (r *redisParticipant) currentStateHandler(instance string, currentState []*model.CurrentState, ctx *helix.Context) {
	log.Info("current state[%s] %+v", instance, currentState)
}
