package task

import (
	"sync"

	"github.com/smiecj/go_common/db"
	"github.com/smiecj/go_common/db/impala"
)

type impalaTableTask struct {
	impalaOption      impala.ImpalaConnectOption
	dbConnector       db.RDBConnector
	connectorInitLock sync.Once
	db                string
	table             string
}

func (task *impalaTableTask) DBConnector() db.RDBConnector {
	task.connectorInitLock.Do(func() {
		task.dbConnector, _ = impala.GetImpalaConnectorByOption(task.impalaOption)
	})
	return task.dbConnector
}

func (task *impalaTableTask) DB() string {
	return task.db
}

func (task *impalaTableTask) Table() string {
	return task.table
}

func (task *impalaTableTask) Finish() error {
	return task.dbConnector.Close()
}
