test:
	go test -cover -covermode=count -coverprofile=cover.out

cover:test
	go tool cover -func=cover.out

report:test
	go tool cover -html=cover.out 

testv:
	go test -cover -covermode=count -v

race:
	go test -race -v

vet:
	go vet ./...

lint:
	golint | grep -v 'comment or be unexported'
