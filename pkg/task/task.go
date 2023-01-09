package task

import (
	"github.com/smiecj/go_common/db"
)

// 一个数据表（统计）任务
type TableTask interface {
	DBConnector() db.RDBConnector
	DB() string
	Table() string
	Finish() error
}

// 任务管理器，可批量获取需要统计的任务
type TaskManager interface {
	GetTaskArr() []TableTask
}
