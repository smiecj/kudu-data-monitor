package monitorjob

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/conf"
	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/countjobmanager"
	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/countjobmanager/impala"
	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/countjobmanager/mysql"
	"github.com/smiecj/go_common/util/alert"
	"github.com/smiecj/go_common/util/log"
	"github.com/smiecj/go_common/util/monitor"
)

// 数据量对比阈值（包括 本身count 阈值和 百分比阈值）
type countDiffThreshold struct {
	count      int
	precentage float32
}

func (threshold *countDiffThreshold) IsExceedThreshold(count1, count2 int) bool {
	diff := math.Abs(float64(count1) - float64(count2))
	if diff > float64(threshold.count) {
		return true
	}
	if diff/float64(count1) > float64(threshold.precentage) {
		return true
	}
	return false
}

const (
	alertMsgJobTimeout  = "本轮任务超时"
	errMsgAlerterIsNil  = "alerter 未初始化"
	errMsgIntervalIsNil = "监控任务周期为空"

	metricsTimeCost     = "job_timecost_gauge"
	metricsExecuteCount = "job_executed_count"
)

var (
	countMonitorJobInitOnce sync.Once
	countMonitorJobInstance monitorJob

	// 数据量差异量对比阈值暂时写死，后续可以放到配置文件中
	countThreshold = countDiffThreshold{
		count:      10,
		precentage: 0.05,
	}

	// 发送周报的日期暂时写死，周天发一次
	summaryReportSendWeekDay   = time.Sunday
	summaryReportSendHour      = 9
	currentPeriodHasSendReport = false
)

// mysql 和 impala 计数监控器实现
type countMonitorJob struct {
	conf *monitorJobConf

	// alert and summary title
	alertTitle   string
	summaryTitle string

	// mysql and impala count job manager
	mysqlCountJobManager  countjobmanager.CountJobManager
	impalaCountJobManager countjobmanager.CountJobManager

	// mysql and impala conf manager
	mysqlConfManager  *conf.MysqlConfManager
	impalaConfManager *conf.ImpalaConfManager

	// monitor: time cost and execute count
	timeCostGauge *monitor.PrometheusGauge
	executedCount *monitor.PrometheusCounter
}

// 初始化任务执行器
func (job *countMonitorJob) init(jobConf *monitorJobConf) error {
	// 进行任务执行前的配置检查
	if jobConf.alerter == nil {
		log.Error("[countMonitorJob.init] " + errMsgAlerterIsNil)
		return fmt.Errorf(errMsgAlerterIsNil)
	}

	if jobConf.interval == 0 {
		log.Error("[countMonitorJob.init] " + errMsgIntervalIsNil)
		return fmt.Errorf(errMsgIntervalIsNil)
	}

	// 初始化配置管理器
	job.conf = jobConf
	job.mysqlConfManager = conf.GetMySQLConfManager()
	job.impalaConfManager = conf.GetImpalaConfManager()
	job.mysqlCountJobManager = mysql.GetCountJobManager()
	job.impalaCountJobManager = impala.GetCountJobManager()

	// 初始化监控指标
	monitorManager := conf.GetMonitorManager()
	monitorManager.AddMetrics(monitor.NewMonitorMetrics(monitor.Gauge, metricsTimeCost, metricsTimeCost, monitor.LabelKey{}))
	monitorManager.AddMetrics(monitor.NewMonitorMetrics(monitor.Counter, metricsExecuteCount, metricsExecuteCount, monitor.LabelKey{}))

	timeCostGaugeObj, _ := monitorManager.GetMetrics(metricsTimeCost)
	executedCountObj, _ := monitorManager.GetMetrics(metricsExecuteCount)
	job.timeCostGauge = timeCostGaugeObj.(*monitor.PrometheusGauge)
	job.executedCount = executedCountObj.(*monitor.PrometheusCounter)

	// 初始化通知标题
	job.alertTitle = fmt.Sprintf("[alert] count_monitor_job env_%s", jobConf.env)
	job.summaryTitle = fmt.Sprintf("[summary] count_monitor_job env_%s", jobConf.env)
	return nil
}

// 开启定期检查告警数协程
func (job *countMonitorJob) Start() error {
	ticker := time.NewTicker(job.conf.interval)
	go func() {
		for range ticker.C {
			// timeout ctx
			timeoutCtx, _ := context.WithTimeout(context.Background(), job.conf.timeout)
			startTime := time.Now()

			select {
			case <-timeoutCtx.Done():
				// timeout 后需要发送告警信息
				log.Warn("[countMonitorJob.Start] timeout: %d", job.conf.timeout)
				job.conf.alerter.Alert(alert.SetAlertTitleAndMsg(
					job.alertTitle, alertMsgJobTimeout),
					alert.SetReceiverArr(job.conf.receiverArr))
			case <-job.execute(timeoutCtx):
				log.Info("[countMonitorJob.Start] compare finish")
			}

			endTime := time.Now()
			// 记录执行耗时，单位秒；执行次数
			job.timeCostGauge.With(monitor.MetricsLabel{}).Set(float64(endTime.Sub(startTime) / time.Second))
			job.executedCount.With(monitor.MetricsLabel{}).Inc()
		}
	}()
	return nil
}

// 执行具体任务
func (job *countMonitorJob) execute(ctx context.Context) <-chan int {
	retChan := make(chan int)
	taskChan := make(chan int)

	go func() {
		select {
		case <-ctx.Done():
		case <-taskChan:
			close(retChan)
		}
	}()

	go func() {
		job.executeCountJob(ctx, taskChan)
	}()

	return retChan
}

// 执行数据量对比任务具体逻辑
func (job *countMonitorJob) executeCountJob(ctx context.Context, taskChan chan int) {
	log.Info("[countMonitorJob.executeCountJob] counter job started")
	// get all mysql table
	mysqlTableArr := job.mysqlConfManager.GetTableArr()
	impalaTableArr := job.impalaConfManager.GetTableArr()
	mysqlCountRetChan := make(chan countjobmanager.TableCountRet)
	impalaCountRetChan := make(chan countjobmanager.TableCountRet)

	// start mysql count job
	go func() {
		job.mysqlCountJobManager.Execute(ctx, mysqlTableArr, mysqlCountRetChan)
	}()

	// start impala count job
	go func() {
		job.impalaCountJobManager.Execute(ctx, impalaTableArr, impalaCountRetChan)
	}()

	// wait for mysql and impala count job both finish
	wg := sync.WaitGroup{}
	wg.Add(2)

	mysqlTableCountMap := make(map[string]int)
	impalaTableCountMap := make(map[string]int)
	tableNameMap := job.mysqlConfManager.GetTableNameMap()
	go func() {
		for mysqlCountRet := range mysqlCountRetChan {
			mysqlTableCountMap[mysqlCountRet.GetTableName()] = mysqlCountRet.Count
		}
		wg.Done()
	}()

	go func() {
		for impalaCountRet := range impalaCountRetChan {
			impalaTableCountMap[impalaCountRet.GetTableName()] = impalaCountRet.Count
		}
		wg.Done()
	}()

	wg.Wait()

	// compare whether mysql table and impala table count same
	alertMsgBuf := bytes.Buffer{}
	for mysqlTableName, mysqlDataCount := range mysqlTableCountMap {
		impalaTableName := tableNameMap[mysqlTableName]
		if impalaTableName == "" {
			alertMsgBuf.WriteString(fmt.Sprintf("mysql table: %s cannot match impala table\n", mysqlTableName))
		} else {
			impalaDataCount := impalaTableCountMap[impalaTableName]

			// 判断数据量不一致的条件: 数据量相差超过指定值 or 超过指定百分比
			if countThreshold.IsExceedThreshold(mysqlDataCount, impalaDataCount) {
				alertMsgBuf.WriteString(fmt.Sprintf("mysql table: %s, impala table: %s, data count: %d -> %d\n",
					mysqlTableName, impalaTableName, mysqlDataCount, impalaDataCount))
			}
		}
	}

	// send alert msg
	if alertMsgBuf.Len() > 0 {
		log.Warn("[countMonitorJob.executeCountJob] ready to send alert msg, title: %s, msg: %s",
			job.alertTitle,
			alertMsgBuf.String())
		job.conf.alerter.Alert(
			alert.SetAlertTitleAndMsg(job.alertTitle, alertMsgBuf.String()),
			alert.SetReceiverArr(job.conf.receiverArr))
	}

	job.sendSummaryReport(mysqlTableCountMap, impalaTableCountMap, tableNameMap)
	close(taskChan)
}

// 发送周报，每周天发送一次（服务重启后状态会被重置）
func (job *countMonitorJob) sendSummaryReport(mysqlTableCountMap, impalaTableCountMap map[string]int, tableNameMap map[string]string) {
	if !currentPeriodHasSendReport && time.Now().Weekday() == summaryReportSendWeekDay && time.Now().Hour() == summaryReportSendHour {
		currentPeriodHasSendReport = true
		log.Info("[countMonitorJob.sendSummaryReport] ready to create and send summary report")

		summaryContentBuf := bytes.Buffer{}
		for mysqlTableName, mysqlDataCount := range mysqlTableCountMap {
			impalaTableName := tableNameMap[mysqlTableName]
			if impalaTableName == "" {
				summaryContentBuf.WriteString(fmt.Sprintf("mysql table: %s cannot match impala table\n", mysqlTableName))
			} else {
				impalaDataCount := impalaTableCountMap[impalaTableName]
				summaryContentBuf.WriteString(fmt.Sprintf("mysql table: %s, impala table: %s, data count: %d -> %d\n",
					mysqlTableName, impalaTableName, mysqlDataCount, impalaDataCount))
			}
		}

		job.conf.alerter.Alert(
			alert.SetAlertTitleAndMsg(job.summaryTitle, summaryContentBuf.String()),
			alert.SetReceiverArr(job.conf.receiverArr))
	} else if time.Now().Weekday() != summaryReportSendWeekDay {
		// 到第二天了要重置 发送状态
		currentPeriodHasSendReport = false
	}
}

// 获取数据对比任务执行器 单例
func GetCountMonitorJob(confSetterArr ...monitorJobConfSetter) monitorJob {
	countMonitorJobInitOnce.Do(func() {
		countMonitorJob := new(countMonitorJob)

		conf := new(monitorJobConf)
		for _, setter := range confSetterArr {
			setter(conf)
		}
		err := countMonitorJob.init(conf)
		if nil != err {
			log.Error("[countMonitorJobInstance] err: %s", err.Error())
			os.Exit(1)
		}

		countMonitorJobInstance = countMonitorJob
	})

	return countMonitorJobInstance
}
