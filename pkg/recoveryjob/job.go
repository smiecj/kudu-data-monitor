package recoveryjob

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"time"

	tools "github.com/smiecj/datalink-tools"
	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/db"
	"github.com/smiecj/go_common/db/impala"
	"github.com/smiecj/go_common/errorcode"
	"github.com/smiecj/go_common/util/alert"
	"github.com/smiecj/go_common/util/log"
	timeutil "github.com/smiecj/go_common/util/time"
	"github.com/smiecj/go_common/zk"
)

const (
	spaceRecovery = "recovery"

	minLazy       = 60
	maxConcurrent = 5
	jobChanSize   = 10

	alertTitle = "[alert] recovery_job"
)

var (
	jobManagerOnce     sync.Once
	jobManagerInstance JobManager
)

type recoveryConf struct {
	Enable        bool   `yaml:"enable"`
	AddTimeout    int    `yaml:"add_timeout"`
	JobTimeout    int    `yaml:"job_timeout"`
	JobChanSize   int    `yaml:"job_chan_size"`
	NotBeforeHour int    `yaml:"not_before_hour"`
	Concurrent    int    `yaml:"concurrent"`
	Lazy          int    `yaml:"lazy"`
	ToolPath      string `yaml:"tool_path"`
	TaskZkPath    string `yaml:"task_zk_path"`
	Job           struct {
		SourceDB string `yaml:"source_db"`
	} `yaml:"job"`
}

func (conf recoveryConf) zkPath(taskId int) string {
	return fmt.Sprintf("%s/%d", conf.TaskZkPath, taskId)
}

func (conf recoveryConf) isBeforeHour() bool {
	return time.Now().Hour() < conf.NotBeforeHour
}

func (conf *recoveryConf) check(countInterval time.Duration) {
	if conf.Lazy < minLazy {
		conf.Lazy = minLazy
	}

	// lazy 必须至少大于统计周期的三倍，否则意义不大 (加上再判断，至少不会连续两次被判断成需要恢复)
	minLazyByCountInterval := 3 * countInterval / time.Minute
	if time.Duration(conf.Lazy) < minLazyByCountInterval {
		conf.Lazy = int(minLazyByCountInterval)
	}

	log.Info("[recoveryConf.check] lazy minute: %d", conf.Lazy)

	if conf.Concurrent > maxConcurrent {
		conf.Concurrent = maxConcurrent
	}
}

type JobConf struct {
	// SourceDB string
	SourceTable string
	TargetDB    string
	TargetTable string
	TaskId      int
}

func (conf JobConf) check() (err error) {
	if conf.SourceTable == "" || conf.TargetDB == "" || conf.TargetTable == "" {
		err = errorcode.BuildErrorWithMsg(errorcode.ServiceError, "db or table name empty")
	}
	return
}

func (conf JobConf) fullTargetTable() string {
	return fmt.Sprintf("%s.%s", conf.TargetDB, conf.TargetTable)
}

// recoveryJob: 一个恢复任务
type recoveryJob interface {
	Begin()
	Finish() <-chan error
}

// recoveryJobImpl: 恢复任务实现
type recoveryJobImpl struct {
	jobConf         JobConf
	ctx             context.Context
	impalaConnector db.RDBConnector
	zkClient        zk.Client
	datalinkClient  tools.Client
	ch              chan error
	once            sync.Once
	recoveryConf    recoveryConf
	log             log.Logger
}

func (impl *recoveryJobImpl) String() string {
	return fmt.Sprintf("recovery table: %s", impl.jobConf.fullTargetTable())
}

type jobFunc func() error

func (impl *recoveryJobImpl) Begin() {
	impl.once.Do(func() {
		go func() {
			jobFuncArr := make([]jobFunc, 0)

			// 1. datalink: 停止任务
			// 停止任务的失败可忽略 （任务本身就是停止状态）
			jobFuncArr = append(jobFuncArr, func() error {
				_ = impl.datalinkClient.StopTask(impl.jobConf.TaskId)
				return nil
			})

			// 2. zk: 清理任务对应的 zk 数据
			// zk 删除节点失败可忽略（已经被删除）
			jobFuncArr = append(jobFuncArr, func() error {
				_ = impl.zkClient.DeleteAll(zk.SetPath(impl.recoveryConf.zkPath(impl.jobConf.TaskId)))
				return nil
			})

			// 3. datalink: 更新任务时间戳
			jobFuncArr = append(jobFuncArr, func() error {
				taskDetail, err := impl.datalinkClient.GetTask(impl.jobConf.TaskId)
				if nil != err {
					return err
				}
				taskDetail.SetTimestamp(timeutil.GetCurrentDateZeroTimestmapMill())
				return impl.datalinkClient.UpdateTask(taskDetail)
			})

			// 4. kudu-tools: 执行 kudu-tools 的脚本，重建表 + 导入最新数据
			jobFuncArr = append(jobFuncArr, impl.recreateTable)

			// 5. datalink: 刷新 worker 服务内的 kudu 表元数据 (kudu java client)
			jobFuncArr = append(jobFuncArr, func() error {
				return impl.datalinkClient.Refresh()
			})

			// 6. datalink: 启动任务
			jobFuncArr = append(jobFuncArr, func() error {
				return impl.datalinkClient.StartTask(impl.jobConf.TaskId)
			})

			err := impl.execSeqJob(jobFuncArr)
			if nil != err {
				impl.ch <- err
			}

			close(impl.ch)
		}()
	})
}

// execSeqJob: 串行执行任务，一个任务报错直接返回
func (impl *recoveryJobImpl) execSeqJob(jobFuncArr []jobFunc) (retErr error) {
	for _, currentFunc := range jobFuncArr {
		retErr = currentFunc()
		if nil != retErr {
			impl.log.Warn("[recoveryJobImpl.execSeqJob] %s 任务执行失败: %s", impl, retErr.Error())
			return
		}
	}
	return
}

// recreateTable: 重建 kudu 表
func (impl *recoveryJobImpl) recreateTable() error {
	// 1. 删除原表
	cmd := exec.CommandContext(impl.ctx, "make", "drop_table")
	cmd.Dir = impl.recoveryConf.ToolPath
	cmd.Env = append(cmd.Env, "db="+impl.jobConf.TargetDB)
	cmd.Env = append(cmd.Env, "table="+impl.jobConf.TargetTable)
	output, err := cmd.Output()
	impl.log.Info("[recoveryJobImpl.recreateTable] drop table: %s", string(output))

	if nil != err {
		impl.log.Warn("[recoveryJobImpl.recreateTable] drop table err: %s, but ignore", err.Error())
	}

	// 2. 重建表
	cmd = exec.CommandContext(impl.ctx, "make", "transform_exec")
	cmd.Dir = impl.recoveryConf.ToolPath
	cmd.Env = append(cmd.Env, "source_db="+impl.recoveryConf.Job.SourceDB)
	cmd.Env = append(cmd.Env, "target_db="+impl.jobConf.TargetDB)
	cmd.Env = append(cmd.Env, "actual_table_name="+impl.jobConf.SourceTable)
	cmd.Env = append(cmd.Env, "target_table="+impl.jobConf.TargetTable)
	output, err = cmd.Output()
	impl.log.Info("[recoveryJobImpl.recreateTable] transform table: %s", string(output))

	if nil != err {
		return err
	}

	// 3. 重新导入数据
	cmd = exec.CommandContext(impl.ctx, "make", "backup_table")
	cmd.Dir = impl.recoveryConf.ToolPath
	cmd.Env = append(cmd.Env, "source_db="+impl.recoveryConf.Job.SourceDB)
	cmd.Env = append(cmd.Env, "target_db="+impl.jobConf.TargetDB)
	cmd.Env = append(cmd.Env, "actual_table_name="+impl.jobConf.TargetTable)
	output, err = cmd.Output()
	impl.log.Info("[recoveryJobImpl.recreateTable] backup table %s : %s", impl.jobConf.TargetTable, string(output))

	if nil != err {
		return err
	}

	return nil
}

func (impl *recoveryJobImpl) Finish() <-chan error {
	return impl.ch
}

// recoveryJobManager: 恢复任务管理器
type JobManager interface {
	Start() error
	Add(JobConf) error
}

// 未启动 recovery job 的空实现
type emptyJobManagerImpl struct{}

func (impl *emptyJobManagerImpl) Add(_ JobConf) error {
	return errorcode.BuildErrorWithMsg(errorcode.ServiceError, "emptyJobManagerImpl will not add job")
}

func (impl *emptyJobManagerImpl) Start() error {
	return nil
}

type jobManagerImpl struct {
	conf            recoveryConf
	jobChan         chan recoveryJob
	impalaConnector db.RDBConnector
	zkClient        zk.Client
	datalinkClient  tools.Client
	// 记录指定表被标记成“需要重启”的时间戳
	lazyTableNameMap map[string]int64
	// 记录指定表 被连续标记的次数，默认第二次才会进行重启
	lazyTableNameMarkCountMap map[string]int
	lazyLock                  sync.RWMutex
	// 失败任务告警发送
	alerter alert.Alerter
}

func (impl *jobManagerImpl) Add(jobConf JobConf) error {
	err := impl.addToLazyMap(jobConf)
	if nil != err {
		return err
	}

	if impl.conf.isBeforeHour() {
		return errorcode.BuildErrorWithMsg(errorcode.ServiceError, "will not add job before hour")
	}

	job := new(recoveryJobImpl)
	job.jobConf = jobConf
	job.ctx, _ = context.WithTimeout(context.Background(), time.Duration(impl.conf.JobTimeout)*time.Second)
	job.impalaConnector = impl.impalaConnector
	job.zkClient = impl.zkClient
	job.datalinkClient = impl.datalinkClient
	job.ch = make(chan error)
	job.recoveryConf = impl.conf
	job.log = log.PrefixLogger(jobConf.fullTargetTable())
	log.Info("[jobManagerImpl.Add] ready to add job, table: %s", jobConf.fullTargetTable())

	go func() {
		select {
		case impl.jobChan <- job:
			log.Info("[jobManagerImpl.Add] add job finish, table: %s", job.jobConf.fullTargetTable())
		case <-time.After(time.Duration(job.recoveryConf.AddTimeout) * time.Second):
			log.Warn("[jobManagerImpl.Add] add job timeout, table: %s", job.jobConf.fullTargetTable())
			impl.alerter.Alert(alert.SetAlertTitleAndMsg(alertTitle, fmt.Sprintf("table: %s, add timeout", job.jobConf.fullTargetTable())))
		}
	}()

	return nil
}

func (impl *jobManagerImpl) Start() error {
	// 并发恢复任务
	for index := 0; index < impl.conf.Concurrent; index++ {
		go func() {
			for {
				job := <-impl.jobChan
				job.Begin()
				err := <-job.Finish()
				if nil != err {
					impl.alerter.Alert(alert.SetAlertTitleAndMsg(alertTitle,
						fmt.Sprintf("job: %s, recover failed: %s", job, err.Error())))
				}
			}
		}()
	}

	// 更新 不重复重启任务的列表
	go func() {
		ticker := time.NewTicker(time.Duration(impl.conf.Lazy) * time.Minute)
		for range ticker.C {
			impl.updateLazyMap()
		}
	}()

	return nil
}

func (impl *jobManagerImpl) updateLazyMap() {
	impl.lazyLock.Lock()
	defer impl.lazyLock.Unlock()

	currentUnix := time.Now().Unix()
	lazyDiff := int64(impl.conf.Lazy) * 60
	for currentTableName, currentTimestamp := range impl.lazyTableNameMap {
		// 超过限定时间的表 进行移除
		if currentUnix-currentTimestamp > lazyDiff {
			delete(impl.lazyTableNameMap, currentTableName)
			delete(impl.lazyTableNameMarkCountMap, currentTableName)
		}
	}
}

func (impl *jobManagerImpl) addToLazyMap(jobConf JobConf) (err error) {
	impl.lazyLock.Lock()
	defer impl.lazyLock.Unlock()

	tableName := jobConf.fullTargetTable()
	// 第一次存入 lazyMap, 只记录时间，不进行处理
	if impl.lazyTableNameMarkCountMap[tableName] == 0 {
		impl.lazyTableNameMap[tableName] = time.Now().Unix()
		err = fmt.Errorf("table: %s first time store in lazy map", tableName)
	} else if impl.lazyTableNameMarkCountMap[tableName] > 1 {
		// 第二次存入, 将会处理
		// 后续存入都不处理，直到超过 lazyTime 被重置
		err = fmt.Errorf("table: %s has recovery once in this period, will not add to lazy", tableName)
	}
	impl.lazyTableNameMarkCountMap[tableName]++
	return
}

func GetJobManager(configManager config.Manager, alerter alert.Alerter, countInterval time.Duration) JobManager {
	jobManagerOnce.Do(func() {
		conf := recoveryConf{}
		configManager.Unmarshal(spaceRecovery, &conf)
		if conf.Enable {
			impl := new(jobManagerImpl)
			impl.conf = conf
			impl.conf.check(countInterval)

			impl.jobChan = make(chan recoveryJob, conf.JobChanSize)

			impl.impalaConnector, _ = impala.GetImpalaConnector(configManager)
			impl.zkClient, _ = zk.GetZKonnector(configManager)
			impl.datalinkClient = tools.GetDataLinkClient(configManager)
			impl.lazyTableNameMap = make(map[string]int64)
			impl.lazyTableNameMarkCountMap = make(map[string]int)
			impl.alerter = alerter

			jobManagerInstance = impl
			impl.Start()
		} else {
			jobManagerInstance = &emptyJobManagerImpl{}
		}
	})

	return jobManagerInstance
}
