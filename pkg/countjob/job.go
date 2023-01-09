// package mysql mysql count 任务实现
package countjob

import (
	"context"
	"sync"

	"github.com/smiecj/go_common/db"
	"github.com/smiecj/go_common/util/log"
	"github.com/smiecj/kudu-data-monitor/pkg/task"
)

type managerImpl struct {
	name   string
	logger log.Logger
}

// 统计表数 具体实现
func (manager *managerImpl) Execute(ctx context.Context, taskArr []task.TableTask, retChan chan<- TableCountRet) error {
	// waitGroup: 确定所有结果都返回才关闭 ret chan
	// chan with size: 控制并发量
	// context: 控制超时
	if len(taskArr) == 0 {
		close(retChan)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(taskArr))
	taskChan := make(chan task.TableTask)

	// producer
	go func() {
		for _, currentTask := range taskArr {
			taskChan <- currentTask
		}
		close(taskChan)
	}()

	// consumer
	// 默认支持10个并发count任务
	for index := 0; index < 10; index++ {
		go func() {
			isDone := false
			for {
				if isDone {
					break
				}
				select {
				case <-ctx.Done():
					isDone = true
				case currentTask := <-taskChan:
					if nil == currentTask {
						isDone = true
						break
					}
					manager.count(currentTask, retChan)
					wg.Done()
				}
			}
		}()
	}
	wg.Wait()
	close(retChan)

	return nil
}

func (manager *managerImpl) count(task task.TableTask, retChan chan<- TableCountRet) {
	defer task.Finish()
	connector := task.DBConnector()
	if connector == nil {
		manager.logger.Warn("[Execute] current db get db connector failed: %s", task.DB())
		// 存入数据为0的记录
		retChan <- TableCountRet{db: task.DB(), table: task.Table(), count: 0}
		return
	}
	ret, err := connector.Count(db.SearchSetSpace(task.DB(), task.Table()))
	if nil != err {
		manager.logger.Warn("[Execute] count failed: %s", err.Error())
		retChan <- TableCountRet{db: task.DB(), table: task.Table(), count: 0}
		return
	}
	retChan <- TableCountRet{db: task.DB(), table: task.Table(), count: ret.Total}
}

// 获取 job manager
func NewCountJobManager(name string) CountJobManager {
	jobManager := managerImpl{
		name:   name,
		logger: log.PrefixLogger(name),
	}
	return &jobManager
}
