package conf

import (
	"testing"

	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/util/file"
	"github.com/smiecj/go_common/util/log"
	"github.com/stretchr/testify/require"
)

const (
	configFileFileManager     = "local_file_conf.yml"
	configFileDatalinkManager = "local_datalink_conf.yml"
)

func TestFileConf(t *testing.T) {
	configFilePath := file.FindFilePath(configFileFileManager)
	testCheckDBAndTableConf(t, configFilePath)
}

func TestDatalinkConf(t *testing.T) {
	configFilePath := file.FindFilePath(configFileDatalinkManager)
	testCheckDBAndTableConf(t, configFilePath)
}

func testCheckDBAndTableConf(t *testing.T, configFilePath string) {
	configManager, _ := config.GetYamlConfigManager(configFilePath)
	confManager := GetConfManager(configManager)
	dbConf := confManager.GetDBConf()
	tableConf := confManager.GetTableMatchConf()
	impalaConf := confManager.GetImpalaConf()
	tableNameMap := confManager.GetTableNameMap()

	log.Info("[test] table name map: %v", tableNameMap)

	require.NotEmpty(t, dbConf.DBInfo)
	require.NotEmpty(t, tableConf.TableArr)
	require.NotEmpty(t, impalaConf.Host)
	require.NotEmpty(t, tableNameMap)
}
