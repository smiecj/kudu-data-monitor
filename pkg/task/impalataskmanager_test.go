package task

import (
	"testing"

	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/util/file"
	"github.com/stretchr/testify/require"
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

func TestImpalaConfManager(t *testing.T) {
	// 测试获取impala连接，不报错即可
	for _, currentConfigFilePath := range toTestConfigFileArr {
		configManager, _ := config.GetYamlConfigManager(currentConfigFilePath)
		taskManager := GetImpalaTaskManager(configManager)

		for _, currentTask := range taskManager.GetTaskArr() {
			connector := currentTask.DBConnector()
			require.NotNil(t, connector)
		}
	}
}
