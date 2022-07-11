build:
	go build -o bin/main cmd/main.go

run:
	./bin/main --conf=/tmp/config.yml