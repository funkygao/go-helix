package command

import (
	log "github.com/funkygao/log4go"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func setupLogging(level string) {
	logLevel := log.ToLogLevel(level, log.DEBUG)
	for _, filter := range log.Global {
		filter.Level = logLevel
	}
}
