package main

import (
	"flag"
	"os"
	"strings"
	"sync"
	"time"

	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/conf"
	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/monitorjob"
	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/util/alert"
	"github.com/smiecj/go_common/util/log"
	"github.com/smiecj/go_common/util/mail"
	"github.com/smiecj/go_common/util/monitor"
	"github.com/smiecj/go_common/util/net"
)

type mailConfig struct {
	Sender   string `yaml:"sender"`
	Token    string `yaml:"token"`
	Receiver string `yaml:"receiver"`
}

type jobConfig struct {
	Interval int    `yaml:"interval"`
	Timeout  int    `yaml:"timeout"`
	Env      string `yaml:"env"`
}

const (
	configSpaceMonitor = "monitor"
	configSpaceMail    = "mail"
	configSpaceJob     = "job"

	configKeyPort = "port"
)

var (
	configFilePath = ""
)

func init() {
	flag.StringVar(&configFilePath, "conf", "config.yml", "config file full path")
	flag.Parse()
}

// main: 启动任务
func main() {

	conf.SetConfigFilePath(configFilePath)
	configManager, _ := config.GetYamlConfig(configFilePath)

	startPrometheusMetrics(configManager)

	startJob(configManager)

	// 设置一个 wait group 不让 main 方法退出
	wg := sync.WaitGroup{}
	wg.Add(1)

	wg.Wait()
}

// 启动 prometheus metrics 接口，进行一些基本的任务指标监控用
func startPrometheusMetrics(configManager config.Manager) {
	monitorPortObj, _ := configManager.Get(configSpaceMonitor, configKeyPort)
	monitorPort := monitorPortObj.(int)

	// 检查端口是否被占用，如果被占用直接退出
	if net.CheckLocalPortIsUsed(monitorPort) {
		log.Error("[startPrometheusMetrics] monitor port: %d is used, program will exit", monitorPort)
		os.Exit(1)
	}
	promethemsMonitor := monitor.GetPrometheusMonitorManager(monitor.PrometheusMonitorManagerConf{
		Port: monitorPort,
	})
	conf.SetMonitorManager(promethemsMonitor)

	// 等待1s 确保接口启动
	time.Sleep(time.Second)
}

func startJob(configManager config.Manager) {
	mailConfig := mailConfig{}
	jobConfig := jobConfig{}
	_ = configManager.Unmarshal(configSpaceMail, &mailConfig)
	_ = configManager.Unmarshal(configSpaceJob, &jobConfig)

	sender := mail.NewQQMailSender(mail.MailSenderConf{
		Sender: mailConfig.Sender,
		Token:  mailConfig.Token,
	})

	// 解析多个告警接收人
	var receiverArr []string
	if strings.Contains(mailConfig.Receiver, ",") {
		receiverArr = strings.Split(mailConfig.Receiver, ",")
	} else {
		receiverArr = []string{mailConfig.Receiver}
	}

	alerter := alert.GetMailAlerter(sender)
	job := monitorjob.GetCountMonitorJob(
		monitorjob.SetMonitorInterval(jobConfig.Interval, jobConfig.Timeout),
		monitorjob.SetAlerter(alerter),
		monitorjob.SetReceiverArr(receiverArr),
		monitorjob.SetEnv(jobConfig.Env))
	_ = job.Start()
}
