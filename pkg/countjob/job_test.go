package countjob

import (
	"context"
	"sync"
	"testing"

	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/util/file"
	"github.com/smiecj/go_common/util/log"
	"github.com/smiecj/kudu-data-monitor/pkg/task"
)

var (
	toTestConfigFileArr []string
)

func init() {
	configFileArr := []string{"local_file_conf.yml", "local_datalink_conf.yml"}
	for _, currentConfigFile := range configFileArr {
		toTestConfigFileArr = append(toTestConfigFileArr, file.FindFilePath(currentConfigFile))
	}
}

// TestCountJob: 分别测试 mysql、impala 的 count job
func TestCountJob(t *testing.T) {
	for _, currentConfigFile := range toTestConfigFileArr {
		configManager, _ := config.GetYamlConfigManager(currentConfigFile)
		log.Info("[test] current config file: %s", currentConfigFile)
		mysqlJobManager := NewCountJobManager("mysql")
		impalaJobManager := NewCountJobManager("impala")

		mysqlTaskManager := task.GetMySQLTaskManager(configManager)
		impalaTaskManager := task.GetImpalaTaskManager(configManager)

		mysqlCountRetChan := make(chan TableCountRet)
		impalaCountRetChan := make(chan TableCountRet)
		mysqlTaskArr := mysqlTaskManager.GetTaskArr()
		impalaTaskArr := impalaTaskManager.GetTaskArr()
		log.Info("[TestCountJob] mysql task count: %d, impala task count: %d", len(mysqlTaskArr), len(impalaCountRetChan))
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			_ = mysqlJobManager.Execute(context.Background(), mysqlTaskArr, mysqlCountRetChan)
		}()
		go func() {
			_ = impalaJobManager.Execute(context.Background(), impalaTaskArr, impalaCountRetChan)
		}()

		go func() {
			for currentRet := range mysqlCountRetChan {
				log.Info("[TestCountJob] mysql ret: %s", currentRet)
			}
			wg.Done()
		}()
		go func() {
			for currentRet := range impalaCountRetChan {
				log.Info("[TestCountJob] impala ret: %s", currentRet)
			}
			wg.Done()
		}()
		wg.Wait()
	}
}
