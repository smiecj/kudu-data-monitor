// package task 配置管理
package task

import (
	"sync"

	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/db/mysql"
	"github.com/smiecj/go_common/util/log"
	"github.com/smiecj/kudu-data-monitor/pkg/conf"
)

var (
	mysqlTaskManagerInitOnce sync.Once
	mysqlTaskManagerInstance *mysqlTaskManager
)

// mysql 配置管理 和 连接初始化
type mysqlTaskManager struct {
	confManager conf.ConfManager
	confLock    sync.RWMutex
	taskArr     []mysqlTableTask
}

func (manager *mysqlTaskManager) start() {
	manager.init()
	go func() {
		for range manager.confManager.Update() {
			manager.init()
		}
	}()
}

// init: 解析配置，生成任务列表
func (manager *mysqlTaskManager) init() {
	manager.confLock.Lock()
	defer manager.confLock.Unlock()

	taskArr := make([]mysqlTableTask, 0)

	dbToOptionMap := make(map[string]mysql.MySQLConnectOption)
	for _, currentDBInfo := range manager.confManager.GetDBConf().DBInfo {
		dbToOptionMap[currentDBInfo.DB] = currentDBInfo.GetMySQLConnectOption()
	}

	for _, currentTableMatch := range manager.confManager.GetTableMatchConf().TableArr {
		if option, ok := dbToOptionMap[currentTableMatch.DB]; ok {
			taskArr = append(taskArr, mysqlTableTask{
				mysqlOption: option,
				db:          option.Database,
				table:       currentTableMatch.Source,
			})
		} else {
			log.Warn("[mysqlTaskManager.init] db mark: %s, cannot match mysql database", currentTableMatch.DB)
		}
	}

	manager.taskArr = taskArr
}

func (manager *mysqlTaskManager) GetTaskArr() (retArr []TableTask) {
	// 从配置中心中拿到所有 mysql 表 和 数据源的关联配置，并初始化数据库连接
	manager.confLock.RLock()
	defer manager.confLock.RUnlock()

	// 注意需要返回一批新的 task 列表，否则 db 连接客户端会被复用，导致 database is closed 错误
	for _, currentTask := range manager.taskArr {
		newTask := currentTask
		retArr = append(retArr, &newTask)
	}
	return
}

// 获取 mysql 任务管理单例
func GetMySQLTaskManager(configManager config.Manager) TaskManager {
	mysqlTaskManagerInitOnce.Do(func() {
		mysqlTaskManagerInstance = new(mysqlTaskManager)
		mysqlTaskManagerInstance.confManager = conf.GetConfManager(configManager)
		mysqlTaskManagerInstance.start()
	})
	return mysqlTaskManagerInstance
}
