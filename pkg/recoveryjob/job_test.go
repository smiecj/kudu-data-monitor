package recoveryjob

import (
	"context"
	"flag"
	"testing"
	"time"

	tools "github.com/smiecj/datalink-tools"
	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/db/impala"
	"github.com/smiecj/go_common/util/file"
	"github.com/smiecj/go_common/zk"
	"github.com/stretchr/testify/require"
)

var (
	configFile  = flag.String("config", "local_datalink_conf.yml", "config path")
	sourceTable = flag.String("source", "t_source", "source table")
	targetDB    = flag.String("target_db", "d_real_time", "target table")
	targetTable = flag.String("target_table", "t_target", "target table")
	taskId      = flag.Int("taskId", 1, "task id")
)

func TestExecuteRecoveryJob(t *testing.T) {
	configManager, _ := config.GetYamlConfigManager(file.FindFilePath(*configFile))
	conf := recoveryConf{}
	configManager.Unmarshal(spaceRecovery, &conf)

	job := new(recoveryJobImpl)
	job.jobConf = JobConf{
		SourceTable: *sourceTable,
		TargetDB:    *targetDB,
		TargetTable: *targetTable,
		TaskId:      *taskId,
	}
	job.ctx, _ = context.WithTimeout(context.Background(), time.Duration(conf.JobTimeout)*time.Second)
	job.impalaConnector, _ = impala.GetImpalaConnector(configManager)
	job.zkClient, _ = zk.GetZKonnector(configManager)
	job.datalinkClient = tools.GetDataLinkClient(configManager)
	job.ch = make(chan error)
	job.recoveryConf = conf

	job.Begin()
	err := <-job.Finish()
	require.Nil(t, err)
}
