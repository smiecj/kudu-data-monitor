// package impala impala count 任务实现
package impala

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
	confManager *conf.ImpalaConfManager
}

// impala 统计表数 具体实现
func (manager *managerImpl) Execute(ctx context.Context, tableArr []model.Table, retChan chan<- countjobmanager.TableCountRet) error {
	// waitGroup: 确定所有结果都返回才关闭 ret chan
	// chan with size: 控制并发量
	// context: 控制超时
	wg := sync.WaitGroup{}
	wg.Add(len(tableArr))
	// 默认最多支持10个并发 impala 请求
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
				log.Warn("[impala count job manager] [Execute] table name: %s will not execute because context done", currentTable.Table)
			default:
				ret, err := manager.confManager.GetImpalaConnector().Count(db.SearchSetSpace(currentTable.Database, currentTable.Table))
				if nil != err {
					log.Error("[impala count job manager] [Execute] count failed: %s", err.Error())
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
		manager.confManager = conf.GetImpalaConfManager()
		jobManagerSingleton = manager
	})
	return jobManagerSingleton
}
