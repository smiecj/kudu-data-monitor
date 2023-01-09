// package monitorjob 监控任务定义
package monitorjob

import (
	"time"

	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/util/alert"
	"github.com/smiecj/go_common/util/monitor"
	"github.com/smiecj/kudu-data-monitor/pkg/conf"
)

const (
	minInterval = 10 * time.Minute
	minTimeout  = 30 * time.Second
)

// actual monitor job
type monitorJob interface {
	Start() error
}

// monitor job basic conf
type monitorJobConf struct {
	interval       time.Duration
	timeout        time.Duration
	alerter        alert.Alerter
	confManager    conf.ConfManager
	configManager  config.Manager
	monitorManager monitor.Manager
}

// 获取一个监控任务配置
type monitorJobConfSetter func(*monitorJobConf) error

// 设置监控周期和超时时间，单位秒
func SetMonitorInterval(interval int, timeout int) monitorJobConfSetter {
	intervalDuration, timeoutDuration := time.Duration(interval)*time.Second, time.Duration(timeout)*time.Second
	return func(conf *monitorJobConf) error {
		// 最短周期
		if intervalDuration < minInterval {
			intervalDuration = minInterval
		}

		// 最短超时时间
		if timeoutDuration < minTimeout {
			timeoutDuration = minTimeout
		}

		// 如果超时时间比周期还长，则将超时时间强行设置成和周期相同
		if timeout > interval {
			timeout = interval
		}

		conf.interval, conf.timeout = intervalDuration, timeoutDuration
		return nil
	}
}

// 设置告警发送器
func SetAlerter(alerter alert.Alerter) monitorJobConfSetter {
	return func(conf *monitorJobConf) error {
		conf.alerter = alerter
		return nil
	}
}

// 设置 config manager
func SetConfigManager(configManager config.Manager) monitorJobConfSetter {
	return func(_conf *monitorJobConf) error {
		_conf.configManager = configManager
		_conf.confManager = conf.GetConfManager(configManager)
		return nil
	}
}

// 设置 monitor manager
func SetMonitorManager(monitorManager monitor.Manager) monitorJobConfSetter {
	return func(conf *monitorJobConf) error {
		conf.monitorManager = monitorManager
		return nil
	}
}
