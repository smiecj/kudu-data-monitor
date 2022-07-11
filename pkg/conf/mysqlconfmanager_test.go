package conf

import (
	"testing"

	"github.com/smiecj/go_common/util/log"
	"github.com/stretchr/testify/require"
)

const (
	testDBName          = "db_test"
	testTableName       = "test_student"
	testTargetTableName = "stg_b001_app_outer_data_storage"
)

func TestMySQLConfManager(t *testing.T) {
	// 测试获取数据库连接
	confManager := GetMySQLConfManager()
	connector := confManager.GetConnectorByDB(testDBName)
	require.NotEmpty(t, connector)

	// 测试获取表列表，检查 数据库名 是否正确
	tableArr := confManager.GetTableArr()
	require.NotEmpty(t, tableArr)
	require.Equal(t, testTableName, tableArr[0].Table)

	tableNameMap := confManager.GetTableNameMap()
	require.NotEmpty(t, tableNameMap)
	for key, value := range tableNameMap {
		log.Info("%s --> %s", key, value)
	}
}
