// package mysql mysql count 任务实现
package mysql

import (
	"context"
	"sync"

	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/conf"
	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/countjobmanager"
	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/model"
	"github.com/smiecj/go_common/db"
	"github.com/smiecj/go_common/util/log"
)

var (
	jobManagerSingleton countjobmanager.CountJobManager
	once                sync.Once
)

type managerImpl struct {
	confManager *conf.MysqlConfManager
}

// mysql 统计表数 具体实现
// todo: 和 impala 实现几乎一致，可以看是否有更加简便的实现方式
// 比如：封装成: job + manager
func (manager *managerImpl) Execute(ctx context.Context, tableArr []model.Table, retChan chan<- countjobmanager.TableCountRet) error {
	// waitGroup: 确定所有结果都返回才关闭 ret chan
	// chan with size: 控制并发量
	// context: 控制超时
	wg := sync.WaitGroup{}
	wg.Add(len(tableArr))
	// 默认最多支持10个并发 mysql 请求
	reqChan := make(chan model.Table, 10)

	// producer
	go func() {
		for _, currentTable := range tableArr {
			reqChan <- currentTable
		}
		close(reqChan)
	}()

	// consumer
	for currentTable := range reqChan {
		go func(currentTable model.Table) {
			select {
			case <-ctx.Done():
				log.Warn("[mysql count job manager] [Execute] table name: %s will not execute because context done", currentTable.Table)
			default:
				connector := manager.confManager.GetConnectorByDB(currentTable.DB)
				if connector == nil {
					log.Warn("[mysql count job manager] [Execute] current db has no connector: %s", currentTable.DB)
					return
				}
				ret, err := connector.Count(db.SearchSetSpace(currentTable.Database, currentTable.Table))
				if nil != err {
					log.Error("[mysql count job manager] [Execute] count failed: %s", err.Error())
				}
				retChan <- countjobmanager.TableCountRet{Table: currentTable, Count: ret.Total}
			}
			wg.Done()
		}(currentTable)
	}

	wg.Wait()
	close(retChan)

	return nil
}

// 获取单例 job manager
func GetCountJobManager() countjobmanager.CountJobManager {
	once.Do(func() {
		manager := new(managerImpl)
		manager.confManager = conf.GetMySQLConfManager()
		jobManagerSingleton = manager
	})
	return jobManagerSingleton
}
