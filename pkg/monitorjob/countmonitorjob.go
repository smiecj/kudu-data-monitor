package monitorjob

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/smiecj/go_common/util/alert"
	"github.com/smiecj/go_common/util/log"
	"github.com/smiecj/go_common/util/monitor"
	"github.com/smiecj/kudu-data-monitor/pkg/countjob"
	"github.com/smiecj/kudu-data-monitor/pkg/recoveryjob"
	"github.com/smiecj/kudu-data-monitor/pkg/task"
)

// 数据量对比阈值（包括 本身count 阈值和 百分比阈值）
type countDiffThreshold struct {
	count      int
	precentage float32
}

// 判断数据量不一致的条件: 数据量相差超过指定值 or 超过指定百分比
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
	mysqlCountJobManager  countjob.CountJobManager
	impalaCountJobManager countjob.CountJobManager

	// mysql and impala conf manager
	mysqlTaskManager  task.TaskManager
	impalaTaskManager task.TaskManager

	// monitor: time cost and execute count
	timeCostGauge *monitor.PrometheusGauge
	executedCount *monitor.PrometheusCounter

	// recovery job manager
	recoveryJobManager recoveryjob.JobManager
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
	job.mysqlTaskManager = task.GetMySQLTaskManager(jobConf.configManager)
	job.impalaTaskManager = task.GetImpalaTaskManager(jobConf.configManager)
	job.mysqlCountJobManager = countjob.NewCountJobManager("mysql")
	job.impalaCountJobManager = countjob.NewCountJobManager("impala")

	// 初始化监控指标
	monitorManager := jobConf.monitorManager
	monitorManager.AddMetrics(monitor.NewMonitorMetrics(monitor.Gauge, metricsTimeCost, metricsTimeCost, monitor.LabelKey{}))
	monitorManager.AddMetrics(monitor.NewMonitorMetrics(monitor.Counter, metricsExecuteCount, metricsExecuteCount, monitor.LabelKey{}))

	job.recoveryJobManager = recoveryjob.GetJobManager(jobConf.configManager, jobConf.alerter, jobConf.interval)

	timeCostGaugeObj, _ := monitorManager.GetMetrics(metricsTimeCost)
	executedCountObj, _ := monitorManager.GetMetrics(metricsExecuteCount)
	job.timeCostGauge = timeCostGaugeObj.(*monitor.PrometheusGauge)
	job.executedCount = executedCountObj.(*monitor.PrometheusCounter)

	// 初始化通知标题
	job.alertTitle = fmt.Sprintf("[alert] count_monitor_job")
	job.summaryTitle = fmt.Sprintf("[summary] count_monitor_job")
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
					job.alertTitle, alertMsgJobTimeout))
			case <-job.execute(timeoutCtx):
				log.Info("[countMonitorJob.Start] count job finish")
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
	mysqlTaskArr := job.mysqlTaskManager.GetTaskArr()
	impalaTaskArr := job.impalaTaskManager.GetTaskArr()
	mysqlCountRetChan := make(chan countjob.TableCountRet)
	impalaCountRetChan := make(chan countjob.TableCountRet)
	log.Info("[countMonitorJob.executeCountJob] mysql task count: %d, impala task count: %d", len(mysqlTaskArr), len(impalaTaskArr))

	// start mysql count job
	go func() {
		job.mysqlCountJobManager.Execute(ctx, mysqlTaskArr, mysqlCountRetChan)
	}()

	// start impala count job
	go func() {
		job.impalaCountJobManager.Execute(ctx, impalaTaskArr, impalaCountRetChan)
	}()

	// wait for mysql and impala count job both finish
	wg := sync.WaitGroup{}
	wg.Add(2)

	mysqlTableCountMap := make(map[string]countjob.TableCountRet)
	impalaTableCountMap := make(map[string]countjob.TableCountRet)
	go func() {
		for mysqlCountRet := range mysqlCountRetChan {
			mysqlTableCountMap[mysqlCountRet.FullTable()] = mysqlCountRet
		}
		wg.Done()
	}()

	go func() {
		for impalaCountRet := range impalaCountRetChan {
			impalaTableCountMap[impalaCountRet.FullTable()] = impalaCountRet
		}
		wg.Done()
	}()

	wg.Wait()

	// compare whether mysql table and impala table count same
	alertMsgBuf := bytes.Buffer{}
	tableNameMap := job.conf.confManager.GetTableNameMap()
	toRecoverImpalaRetArr := make([]countjob.TableCountRet, 0)
	for mysqlFullTableName, mysqlCountRet := range mysqlTableCountMap {
		if impalaFullTableName, ok := tableNameMap[mysqlFullTableName]; ok {
			impalaCountRet := impalaTableCountMap[impalaFullTableName]

			mysqlDataCount, impalaDataCount := mysqlCountRet.Count(), impalaCountRet.Count()

			if countThreshold.IsExceedThreshold(mysqlDataCount, impalaDataCount) {
				alertMsgBuf.WriteString(fmt.Sprintf("mysql table: %s, impala table: %s, data count: %d -> %d\n",
					mysqlFullTableName, impalaFullTableName, mysqlDataCount, impalaDataCount))
				toRecoverImpalaRetArr = append(toRecoverImpalaRetArr, impalaCountRet)
			}
		} else {
			alertMsgBuf.WriteString(fmt.Sprintf("mysql table: %s cannot match impala table\n", mysqlFullTableName))
		}
	}

	log.Info("[countMonitorJob.executeCountJob] to recover table countL: %d, %v", len(toRecoverImpalaRetArr), toRecoverImpalaRetArr)
	job.addRecoveryJob(toRecoverImpalaRetArr)

	// 发送汇总后的表数据量告警消息
	if alertMsgBuf.Len() > 0 {
		log.Warn("[countMonitorJob.executeCountJob] ready to send alert msg, title: %s, msg: %s",
			job.alertTitle,
			alertMsgBuf.String())
		job.conf.alerter.Alert(
			alert.SetAlertTitleAndMsg(job.alertTitle, alertMsgBuf.String()))
	}

	job.sendSummaryReport(mysqlTableCountMap, impalaTableCountMap, tableNameMap)
	close(taskChan)
}

// 发送周报，每周发送一次（服务重启后状态会被重置）
func (job *countMonitorJob) sendSummaryReport(mysqlTableCountMap, impalaTableCountMap map[string]countjob.TableCountRet, tableNameMap map[string]string) {
	if !currentPeriodHasSendReport && time.Now().Weekday() == summaryReportSendWeekDay && time.Now().Hour() == summaryReportSendHour {
		currentPeriodHasSendReport = true
		log.Info("[countMonitorJob.sendSummaryReport] ready to create and send summary report")

		summaryContentBuf := bytes.Buffer{}
		for mysqlFullTableName, mysqlDataCountRet := range mysqlTableCountMap {
			impalaFullTableName := tableNameMap[mysqlFullTableName]
			if impalaFullTableName == "" {
				summaryContentBuf.WriteString(fmt.Sprintf("mysql table: %s cannot match impala table\n", mysqlFullTableName))
			} else {
				impalaDataCountRet := impalaTableCountMap[impalaFullTableName]
				mysqlDataCount, impalaDataCount := mysqlDataCountRet.Count(), impalaDataCountRet.Count()
				summaryContentBuf.WriteString(fmt.Sprintf("mysql table: %s, impala table: %s, data count: %d -> %d\n",
					mysqlFullTableName, impalaFullTableName, mysqlDataCount, impalaDataCount))
			}
		}

		job.conf.alerter.Alert(
			alert.SetAlertTitleAndMsg(job.summaryTitle, summaryContentBuf.String()))
	} else if time.Now().Weekday() != summaryReportSendWeekDay {
		// 到第二天了要重置 发送状态
		currentPeriodHasSendReport = false
	}
}

// 添加 recovery job
func (job *countMonitorJob) addRecoveryJob(countRetArr []countjob.TableCountRet) {
	tableTaskIdMap := job.conf.confManager.GetTableTaskMap()
	for _, currentCountRet := range countRetArr {
		// 获取 impala 表对应的 task id
		impalaFullTableName := currentCountRet.FullTable()
		taskId, ok := tableTaskIdMap[impalaFullTableName]
		if !ok {
			log.Warn("[countMonitorJob.addRecoveryJob] impala table: %s, not found datalink task", impalaFullTableName)
			continue
		}
		// 切分库表名

		sourceHiveTable := currentCountRet.Table()
		err := job.recoveryJobManager.Add(recoveryjob.JobConf{
			SourceTable: sourceHiveTable,
			TargetDB:    currentCountRet.DB(),
			TargetTable: currentCountRet.Table(),
			TaskId:      taskId,
		})

		if nil != err {
			log.Warn("[countMonitorJob.addRecoveryJob] table %s recover failed: %s", impalaFullTableName, err.Error())
		}
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
