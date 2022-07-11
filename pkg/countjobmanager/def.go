// package countjobmanager 计数任务管理者定义
package countjobmanager

import (
	"context"

	"git.hrlyit.com/beebird/cdh-migration/kudu-data-monitor/pkg/model"
)

type TableCountRet struct {
	model.Table
	Count int
}

// counter job manager
type CountJobManager interface {
	Execute(context.Context, []model.Table, chan<- TableCountRet) error
}
