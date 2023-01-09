package task

import (
	"sync"

	"github.com/smiecj/go_common/db"
	"github.com/smiecj/go_common/db/mysql"
)

type mysqlTableTask struct {
	mysqlOption       mysql.MySQLConnectOption
	dbConnector       db.RDBConnector
	connectorInitLock sync.Once
	db                string
	table             string
}

func (task *mysqlTableTask) DBConnector() db.RDBConnector {
	task.connectorInitLock.Do(func() {
		task.dbConnector, _ = mysql.GetMySQLConnectorByOption(task.mysqlOption)
	})
	return task.dbConnector
}

func (task *mysqlTableTask) DB() string {
	return task.db
}

func (task *mysqlTableTask) Table() string {
	return task.table
}

func (task *mysqlTableTask) Finish() error {
	return task.dbConnector.Close()
}
