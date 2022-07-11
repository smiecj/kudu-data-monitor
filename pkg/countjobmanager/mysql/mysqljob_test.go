package mysql

import (
	"context"
	"testing"

	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/conf"
	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/countjobmanager"
	"github.com/smiecj/go_common/util/log"
)

func TestMySQLCountJob(t *testing.T) {
	jobManager := GetCountJobManager()
	confManager := conf.GetMySQLConfManager()

	// 进行一次count 数获取
	countRetChan := make(chan countjobmanager.TableCountRet)
	go func() {
		jobManager.Execute(context.Background(), confManager.GetTableArr(), countRetChan)
	}()
	for countRet := range countRetChan {
		log.Info("[test] table name: %s, count: %d", countRet.GetTableName(), countRet.Count)
	}
}