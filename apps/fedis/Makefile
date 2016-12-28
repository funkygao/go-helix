build:
	#@go install -race
	@go install

a:build
	fedis redis -node localhost_10001 -log debug

at:build
	fedis redis -node localhost_10001 -log trace

b:build
	fedis redis -node localhost_10002 -log debug

bt:build
	fedis redis -node localhost_10002 -log trace

trace:build
	@fedis trace -log debug

zk:
	gk zk -z local -c wchp
