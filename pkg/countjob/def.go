// package countjobmanager 计数任务管理者定义
package countjob

import (
	"context"
	"fmt"

	"github.com/smiecj/kudu-data-monitor/pkg/task"
)

type TableCountRet struct {
	db    string
	table string
	count int
}

func (ret TableCountRet) String() string {
	return fmt.Sprintf("[TableCountRet] db: %s, table: %s, count: %d", ret.db, ret.table, ret.count)
}

func (ret *TableCountRet) DB() string {
	return ret.db
}

func (ret *TableCountRet) Table() string {
	return ret.table
}

func (ret *TableCountRet) FullTable() string {
	return fmt.Sprintf("%s.%s", ret.db, ret.table)
}

func (ret *TableCountRet) Count() int {
	return ret.count
}

// counter job manager
type CountJobManager interface {
	Execute(context.Context, []task.TableTask, chan<- TableCountRet) error
}
