// package conf 配置管理
package conf

import (
	"os"
	"sync"

	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/model"
	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/db"
	"github.com/smiecj/go_common/db/mysql"
	"github.com/smiecj/go_common/util/log"
)

var (
	mysqlConfManagerInitOnce sync.Once
	mysqlConfManagerInstance *MysqlConfManager
)

const (
	// 配置相关
	spaceNameTableMatch = "table_match"
	spaceNameDB         = "db"
)

type tableMatchConf struct {
	TableArr []struct {
		Source string `yaml:"source"`
		Target string `yaml:"target"`
		DB     string `yaml:"db"`
	} `yaml:"table_arr"`
}

type dbConf struct {
	DBInfo []struct {
		DB       string `yaml:"db"`
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Database string `yaml:"database"`
	} `yaml:"db_info"`
}

// mysql 配置管理 和 连接初始化
type MysqlConfManager struct {
	dbConnectorMap                     map[string]db.RDBConnector
	tableArr                           []model.Table
	mysqlTableNameToImpalaTableNameMap map[string]string
}

// 初始化配置
func (manager *MysqlConfManager) init() {
	// 从配置中心中拿到所有 mysql 表 和 数据源的关联配置，并初始化数据库连接
	config, _ := config.GetYamlConfig(GetConfigFilePath())
	tableMatchConf := tableMatchConf{}
	dbConf := dbConf{}
	config.Unmarshal(spaceNameTableMatch, &tableMatchConf)
	config.Unmarshal(spaceNameDB, &dbConf)

	// 初始化数据库连接
	// 如果有任何一个数据库无法连接，都直接退出
	// key: 数据库名称标识（注意不是库名）
	manager.dbConnectorMap = make(map[string]db.RDBConnector)
	dbToDatabaseMap := make(map[string]string)
	manager.mysqlTableNameToImpalaTableNameMap = make(map[string]string)
	for _, currentDBInfo := range dbConf.DBInfo {
		log.Info("[MysqlConfManager.init] connecting database: %s:%d", currentDBInfo.Host, currentDBInfo.Port)
		currentConnnector, err := mysql.GetMySQLConnector(mysql.MySQLConnectOption{
			Host:     currentDBInfo.Host,
			Port:     currentDBInfo.Port,
			User:     currentDBInfo.User,
			Password: currentDBInfo.Password,
		})
		if nil != err {
			log.Error("[mysqlConfManager.init] current db: %s:%d connect failed, please check!",
				currentDBInfo.Host, currentDBInfo.Port)
			os.Exit(1)
		}
		log.Info("[MysqlConfManager.init] connect database: %s:%d success", currentDBInfo.Host, currentDBInfo.Port)
		manager.dbConnectorMap[currentDBInfo.DB] = currentConnnector
		dbToDatabaseMap[currentDBInfo.DB] = currentDBInfo.Database
	}

	for _, currentTableMatch := range tableMatchConf.TableArr {
		currentTable := model.Table{
			DB:       currentTableMatch.DB,
			Database: dbToDatabaseMap[currentTableMatch.DB],
			Table:    currentTableMatch.Source,
		}
		manager.tableArr = append(manager.tableArr, currentTable)
		manager.mysqlTableNameToImpalaTableNameMap[currentTable.GetTableName()] =
			model.Table{Database: GetImpalaConfManager().GetDatabaseName(), Table: currentTableMatch.Target}.GetTableName()
	}
}

// 通过 介质名 获取实际的数据库连接
func (manager *MysqlConfManager) GetConnectorByDB(dbName string) db.RDBConnector {
	return manager.dbConnectorMap[dbName]
}

// 获取所有的 mysql 表信息，由 monitor job 主流程调用，触发统计所有表的逻辑
func (manager *MysqlConfManager) GetTableArr() []model.Table {
	log.Info("[debug] MysqlConfManager.GetTableArr, table arr len: %d", len(manager.tableArr))
	retArr := make([]model.Table, len(manager.tableArr))
	copy(retArr, manager.tableArr)
	return retArr
}

// 获取mysql 表名 和 impala 表名的映射map，由 monitor job 主流程调用
func (manager *MysqlConfManager) GetTableNameMap() map[string]string {
	retMap := make(map[string]string, len(manager.mysqlTableNameToImpalaTableNameMap))
	for key, value := range manager.mysqlTableNameToImpalaTableNameMap {
		retMap[key] = value
	}
	return retMap
}

// 获取 mysql 配置管理单例
func GetMySQLConfManager() *MysqlConfManager {
	mysqlConfManagerInitOnce.Do(func() {
		mysqlConfManagerInstance = new(MysqlConfManager)
		mysqlConfManagerInstance.init()
	})
	return mysqlConfManagerInstance
}
