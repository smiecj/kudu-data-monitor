package conf

import "github.com/smiecj/go_common/util/monitor"

var (
	// 一些通用配置，在main 方法初始阶段加载服务的时候就设置好，如配置文件地址
	configFilePath = "/tmp/config.yml"
	monitorManager monitor.Manager
)

func SetMonitorManager(_monitorManager monitor.Manager) {
	monitorManager = _monitorManager
}

func GetMonitorManager() monitor.Manager {
	return monitorManager
}

func SetConfigFilePath(path string) {
	configFilePath = path
}

func GetConfigFilePath() string {
	return configFilePath
}
