ZK=localhost:2181
CLUSTER=fedis
RESOURCE=redis
REPLICA=2

build:
	@go install

init:build
	fedis init -reinit -rebalance SEMI_AUTO

rebalance:
	/opt/app/helix/bin/helix-admin.sh --zkSvr ${ZK}  --rebalance ${CLUSTER} ${RESOURCE} ${REPLICA}

a:build
	fedis redis -node localhost_10001 -log debug

at:build
	fedis redis -node localhost_10001 -log trace

b:build
	fedis redis -node localhost_10002 -log debug

bt:build
	fedis redis -node localhost_10002 -log trace

c:build
	fedis redis -node localhost_10003 -log debug

ct:build
	fedis redis -node localhost_10003 -log trace

trace:build
	@fedis trace -log debug

controller:
	/opt/helix/bin/run-helix-controller.sh --zkSvr ${ZK}  --cluster ${CLUSTER}

zk:
	gk zk -z local -c wchp

