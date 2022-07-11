// package model 一些模型定义
package model

import "fmt"

// 表定义，包含 datalink 上的介质名（用于获取数据库连接）、数据库名和表名
type Table struct {
	DB       string
	Database string
	Table    string
}

// 获取完整表名
func (table Table) GetTableName() string {
	return fmt.Sprintf("`%s`.`%s`", table.Database, table.Table)
}
