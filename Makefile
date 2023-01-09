build:
	go build -o bin/main cmd/main.go

run:
	./bin/main --conf=/tmp/config.yml

clean:
	rm -rf bin

test_job_count:
	go test -timeout 60s -run ^TestCountJob$$ github.com/smiecj/kudu-data-monitor/pkg/countjobmanager -v -count=1

test_recovery:
	go test -timeout 60s -run ^TestExecuteRecoveryJob$$ github.com/smiecj/kudu-data-monitor/pkg/recoveryjob -v -count=1