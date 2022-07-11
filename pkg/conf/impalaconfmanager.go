package conf

import (
	"os"
	"sync"

	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/model"
	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/db"
	"github.com/smiecj/go_common/db/impala"
	"github.com/smiecj/go_common/util/log"
)

const (
	spaceNameImpala = "impala"
)

var (
	impalaConfManagerInitOnce sync.Once
	impalaConfManagerInstance *ImpalaConfManager
)

// impala 连接配置
type ImpalaConf struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
}

// impala 配置管理 和 连接初始化
type ImpalaConfManager struct {
	connector    db.RDBConnector
	tableArr     []model.Table
	databaseName string
}

// 初始化 impala 连接
// 如果impala 连接都无法创建，直接退出
func (manager *ImpalaConfManager) init() {
	configManager, _ := config.GetYamlConfig(GetConfigFilePath())

	manager.initImpalaConnection(configManager)
	manager.initImpalaTableArr(configManager)
}

// 初始化impala 数据库连接
func (manager *ImpalaConfManager) initImpalaConnection(configManager config.Manager) {
	impalaConf := ImpalaConf{}
	configManager.Unmarshal(spaceNameImpala, &impalaConf)

	var err error
	log.Info("[ImpalaConfManager.init] connecting impala %s:%d", impalaConf.Host, impalaConf.Port)
	manager.connector, err = impala.GetImpalaConnector(impala.ImpalaConnectOption{
		Host: impalaConf.Host,
		Port: impalaConf.Port,
	})
	if nil != err {
		log.Error("[ImpalaConfManager.init] connect impala failed, please check: %s", err.Error())
		os.Exit(1)
	}
	log.Info("[ImpalaConfManager.init] connect impala %s:%d success", impalaConf.Host, impalaConf.Port)
	manager.databaseName = impalaConf.Database
}

// 从配置中心解析所有impala 表
func (manager *ImpalaConfManager) initImpalaTableArr(configManager config.Manager) {
	tableMatchConf := tableMatchConf{}
	configManager.Unmarshal(spaceNameTableMatch, &tableMatchConf)

	manager.tableArr = make([]model.Table, 0, len(tableMatchConf.TableArr))
	for _, currentTableMatch := range tableMatchConf.TableArr {
		currentTable := model.Table{
			Database: manager.databaseName,
			Table:    currentTableMatch.Target,
		}
		manager.tableArr = append(manager.tableArr, currentTable)
	}
}

// 获取 impala 数据源
func (manager *ImpalaConfManager) GetImpalaConnector() db.RDBConnector {
	return manager.connector
}

// 获取 impala 连接表名
func (manager *ImpalaConfManager) GetDatabaseName() string {
	return manager.databaseName
}

// 获取所有的 impala 表信息，由任务执行侧调用
func (manager *ImpalaConfManager) GetTableArr() []model.Table {
	return manager.tableArr
}

// 获取 impala 管理管理单例
func GetImpalaConfManager() *ImpalaConfManager {
	impalaConfManagerInitOnce.Do(func() {
		impalaConfManagerInstance = new(ImpalaConfManager)
		impalaConfManagerInstance.init()
	})
	return impalaConfManagerInstance
}
