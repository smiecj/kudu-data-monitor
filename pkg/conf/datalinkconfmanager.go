package conf

import (
	"fmt"
	"sync"
	"time"

	tools "github.com/smiecj/datalink-tools"
	"github.com/smiecj/go_common/config"
)

const (
	minInterval = 600
)

// datalinkConfManager: 存入 client、定期维护映射
type datalinkConfManager struct {
	impalaConf impalaConf

	classConf     confClass
	configManager config.Manager

	client      tools.Client
	mappingLock sync.RWMutex
	mappingArr  []*tools.TaskMapping
	c           chan struct{}
}

// 定期维护库表映射
func (manager *datalinkConfManager) start() {
	// impala 配置保持不变
	manager.configManager.Unmarshal(spaceNameImpala, &manager.impalaConf)

	manager.client = tools.GetDataLinkClient(manager.configManager)
	// 先更新一次配置
	manager.init()
	interval := manager.classConf.Interval
	if interval < minInterval {
		interval = minInterval
	}
	manager.c = make(chan struct{})
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	go func() {
		for range ticker.C {
			manager.init()
		}
	}()
}

// 从 datalink 服务端拉取并更新配置
func (manager *datalinkConfManager) init() {
	task, _ := manager.client.GetTasks()
	mapping, _ := manager.client.GetMappings()
	rdbMedia, _ := manager.client.GetRDBMedias()
	kuduMedia, _ := manager.client.GetKuduMedias()

	mappingArr := tools.GetTaskMapping(manager.client, task, mapping, rdbMedia, kuduMedia)
	manager.mappingLock.Lock()
	defer manager.mappingLock.Unlock()
	// 配置数太少，不进行更新
	if manager.classConf.MinMappingCount != 0 && len(mappingArr) < manager.classConf.MinMappingCount {
		return
	}
	manager.mappingArr = mappingArr
	go func() {
		manager.c <- struct{}{}
	}()
}

func (manager *datalinkConfManager) Update() <-chan struct{} {
	return manager.c
}

// todo: get 方法对 lock 的使用可以封装
func (manager *datalinkConfManager) GetDBConf() (ret dbConf) {
	manager.mappingLock.RLock()
	defer manager.mappingLock.RUnlock()
	for _, currentMapping := range manager.mappingArr {
		ret.DBInfo = append(ret.DBInfo, dbInfo{
			// db 名称自己组合，保证和 getTableMatchConf 得到的是同一个就行
			DB:       manager.getDBName(currentMapping),
			Host:     currentMapping.RDBMedia.GetReadHost(),
			Port:     currentMapping.RDBMedia.GetReadPort(),
			User:     currentMapping.RDBMedia.GetReadUserName(),
			Password: currentMapping.RDBMedia.GetReadPassword(),
			Database: currentMapping.RDBMedia.Schema,
		})
	}
	return
}

func (manager *datalinkConfManager) GetTableMatchConf() (ret tableMatchConf) {
	manager.mappingLock.RLock()
	defer manager.mappingLock.RUnlock()
	for _, currentMapping := range manager.mappingArr {
		ret.TableArr = append(ret.TableArr, table{
			Source: currentMapping.Mapping.SourceTable(),
			Target: currentMapping.Mapping.TargetTable(),
			DB:     manager.getDBName(currentMapping),
		})
	}
	return
}

func (manager *datalinkConfManager) GetTableNameMap() map[string]string {
	manager.mappingLock.RLock()
	defer manager.mappingLock.RUnlock()
	retMap := make(map[string]string)
	for _, currentMapping := range manager.mappingArr {
		retMap[currentMapping.FullSourceTable()] = currentMapping.FullTargetTable()
	}
	return retMap
}

func (manager *datalinkConfManager) GetTableTaskMap() map[string]int {
	manager.mappingLock.RLock()
	defer manager.mappingLock.RUnlock()
	retMap := make(map[string]int)
	for _, currentMapping := range manager.mappingArr {
		retMap[currentMapping.FullTargetTable()] = currentMapping.Task.Id
	}
	return retMap
}

func (manager *datalinkConfManager) GetImpalaConf() impalaConf {
	return manager.impalaConf
}

func (manager *datalinkConfManager) getDBName(mapping *tools.TaskMapping) string {
	return fmt.Sprintf("%s_%s:%d", mapping.RDBMedia.RDBConnectConfig.Name, mapping.RDBMedia.GetReadHost(), mapping.RDBMedia.GetReadPort())
}
