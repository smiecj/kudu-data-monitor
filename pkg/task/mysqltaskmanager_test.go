package task

import (
	"testing"

	"github.com/smiecj/go_common/config"
	"github.com/stretchr/testify/require"
)

func TestMySQLConfManager(t *testing.T) {
	for _, currentConfigFilePath := range toTestConfigFileArr {
		configManager, _ := config.GetYamlConfigManager(currentConfigFilePath)
		taskManager := GetMySQLTaskManager(configManager)

		for _, currentTask := range taskManager.GetTaskArr() {
			connector := currentTask.DBConnector()
			require.NotNil(t, connector)
		}
	}
}
