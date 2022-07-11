package conf

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImpalaConfManager(t *testing.T) {
	// 测试获取impala连接，不报错即可
	confManager := GetImpalaConfManager()
	_ = confManager.GetImpalaConnector()

	// 测试获取 impala 表配置
	tableArr := confManager.GetTableArr()
	require.NotEmpty(t, tableArr)
}
