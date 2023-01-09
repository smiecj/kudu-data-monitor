package task

import (
	"sync"

	"github.com/smiecj/go_common/config"
	"github.com/smiecj/kudu-data-monitor/pkg/conf"
)

var (
	impalaTaskManagerInitOnce sync.Once
	impalaTaskManagerInstance *impalaTaskManager
)

// impala 任务管理
type impalaTaskManager struct {
	confManager conf.ConfManager
	db          string
	tableArr    []string
	confLock    sync.RWMutex
}

func (manager *impalaTaskManager) start() {
	manager.initImpalaTableArr()
	go func() {
		for range manager.confManager.Update() {
			manager.initImpalaTableArr()
		}
	}()
}

func (manager *impalaTaskManager) initImpalaTableArr() {
	manager.confLock.Lock()
	defer manager.confLock.Unlock()

	tableArr := make([]string, 0)
	for _, currentTableMatch := range manager.confManager.GetTableMatchConf().TableArr {
		tableArr = append(tableArr, currentTableMatch.Target)
	}
	manager.tableArr = tableArr
}

func (manager *impalaTaskManager) GetTaskArr() (retArr []TableTask) {
	manager.confLock.RLock()
	defer manager.confLock.RUnlock()
	impalaConf := manager.confManager.GetImpalaConf()
	for _, currentTableName := range manager.tableArr {
		currentTableTask := impalaTableTask{
			impalaOption: impalaConf.GetImpalaConnectOption(),
			db:           impalaConf.Database,
			table:        currentTableName,
		}
		retArr = append(retArr, &currentTableTask)
	}
	return
}

// 获取 impala 任务管理单例
func GetImpalaTaskManager(configManager config.Manager) TaskManager {
	impalaTaskManagerInitOnce.Do(func() {
		impalaTaskManagerInstance = new(impalaTaskManager)
		impalaTaskManagerInstance.confManager = conf.GetConfManager(configManager)
		impalaTaskManagerInstance.start()
	})
	return impalaTaskManagerInstance
}
