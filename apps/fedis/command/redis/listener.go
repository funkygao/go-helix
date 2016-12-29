package redis

import (
	log "github.com/funkygao/log4go"
)

func (r *redisParticipant) setupListener() {
	log.Info("traceing external view to know who is master")
	r.m.AddExternalViewChangeListener(r.redis.Replicator)
}
