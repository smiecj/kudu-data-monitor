package conf

import (
	"fmt"

	"github.com/smiecj/go_common/config"
)

// 从配置文件中读取 mysql 地址和映射信息
type fileConfManager struct {
	configManager  config.Manager
	tableMatchConf tableMatchConf
	dbConf         dbConf
	impalaConf     impalaConf
}

func (manager *fileConfManager) GetDBConf() dbConf {
	return manager.dbConf
}

func (manager *fileConfManager) GetTableMatchConf() tableMatchConf {
	return manager.tableMatchConf
}

func (manager *fileConfManager) GetTableNameMap() map[string]string {
	retMap := make(map[string]string)
	dbMap := make(map[string]dbInfo)
	for _, currentDBInfo := range manager.dbConf.DBInfo {
		dbMap[currentDBInfo.DB] = currentDBInfo
	}
	for _, currentTableMatch := range manager.tableMatchConf.TableArr {
		sourceDB := dbMap[currentTableMatch.DB].Database
		retMap[fmt.Sprintf("%s.%s", sourceDB, currentTableMatch.Source)] = fmt.Sprintf("%s.%s", manager.impalaConf.Database, currentTableMatch.Target)
	}
	return retMap
}

// file conf manager: 无需实现
func (manager *fileConfManager) GetTableTaskMap() map[string]int {
	return map[string]int{}
}

func (manager *fileConfManager) GetImpalaConf() impalaConf {
	return manager.impalaConf
}

// update: 文件配置不需要更新
func (manager *fileConfManager) Update() <-chan struct{} {
	retChan := make(chan struct{})
	return retChan
}

func (manager *fileConfManager) start() {
	// 文件配置一般不会变更，只需要获取一次
	manager.configManager.Unmarshal(spaceNameTableMatch, &manager.tableMatchConf)
	manager.configManager.Unmarshal(spaceNameDB, &manager.dbConf)
	manager.configManager.Unmarshal(spaceNameImpala, &manager.impalaConf)
}
