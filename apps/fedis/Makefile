build:
	@go install -race

a:build
	fedis redis -node localhost_10001 -log debug

b:build
	fedis redis -node localhost_10002 -log debug

trace:build
	@fedis trace -log debug

zk:
	gk zk -z local -c wchp
