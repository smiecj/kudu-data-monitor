package main

import (
	"flag"
	"fmt"
	"sync"

	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/util/alert"
	"github.com/smiecj/go_common/util/mail"
	"github.com/smiecj/go_common/util/monitor"
	"github.com/smiecj/kudu-data-monitor/pkg/monitorjob"
)

type mailConfig struct {
	Sender   string `yaml:"sender"`
	Token    string `yaml:"token"`
	Receiver string `yaml:"receiver"`
}

type countConfig struct {
	Interval int `yaml:"interval"`
	Timeout  int `yaml:"timeout"`
}

type envConfig struct {
	Name string `yaml:"name"`
}

const (
	configSpaceMonitor = "monitor"
	configSpaceCount   = "count"
	configSpaceEnv     = "env"
)

var (
	configFilePath string
)

func init() {
	flag.StringVar(&configFilePath, "conf", "config.yml", "config file full path")
	flag.Parse()
}

// main: 启动任务
func main() {
	configManager, _ := config.GetYamlConfigManager(configFilePath)
	monitorManager := monitor.GetPrometheusMonitorManager(configManager)

	startJob(configManager, monitorManager)

	// wait group 不让 main 方法退出
	wg := sync.WaitGroup{}
	wg.Add(1)

	wg.Wait()
}

func startJob(configManager config.Manager, monitorManager monitor.Manager) {
	countConfig := countConfig{}
	_ = configManager.Unmarshal(configSpaceCount, &countConfig)

	envConfig := envConfig{}
	_ = configManager.Unmarshal(configSpaceEnv, &envConfig)
	sender, _ := mail.NewSMTPMailSender(configManager)
	alerter := alert.GetMailAlerter(sender)
	alerter.SetCommonTitle(fmt.Sprintf("[%s]", envConfig.Name))

	job := monitorjob.GetCountMonitorJob(
		monitorjob.SetMonitorInterval(countConfig.Interval, countConfig.Timeout),
		monitorjob.SetAlerter(alerter),
		monitorjob.SetConfigManager(configManager),
		monitorjob.SetMonitorManager(monitorManager))
	_ = job.Start()
}
