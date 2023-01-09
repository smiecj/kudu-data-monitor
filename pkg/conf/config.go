package conf

import (
	"sync"

	"github.com/smiecj/go_common/config"
	"github.com/smiecj/go_common/db/impala"
	"github.com/smiecj/go_common/db/mysql"
	"github.com/smiecj/go_common/util/monitor"
)

var (
	monitorManager monitor.Manager
	spaceConfClass = "conf_class"

	confManagerClassFile     = "file"
	confManagerClassDatalink = "datalink"

	spaceNameTableMatch = "table_match"
	spaceNameDB         = "db"

	spaceNameImpala = "impala"

	// conf manager 单例
	confManagerInstance ConfManager
	confManagerLock     sync.Once
)

type tableMatchConf struct {
	TableArr []table `yaml:"table_arr"`
}

type dbConf struct {
	DBInfo []dbInfo `yaml:"db_info"`
}

type dbInfo struct {
	// 分别对应: 数据库名称（自命名，不能重复）、数据库host、port、用户名、密码、db名
	DB       string `yaml:"db"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

func (dbInfo dbInfo) GetMySQLConnectOption() mysql.MySQLConnectOption {
	return mysql.MySQLConnectOption{
		Host:     dbInfo.Host,
		Port:     dbInfo.Port,
		User:     dbInfo.User,
		Password: dbInfo.Password,
		Database: dbInfo.Database,
	}
}

type table struct {
	// 分别对应: 源表名、目标表名、数据库名称（对应 dbInfo.DB）
	Source string `yaml:"source"`
	Target string `yaml:"target"`
	DB     string `yaml:"db"`
}

// impala 连接配置
type impalaConf struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
}

func (conf impalaConf) GetImpalaConnectOption() impala.ImpalaConnectOption {
	return impala.ImpalaConnectOption{
		Host: conf.Host,
		Port: conf.Port,
	}
}

// config manager
type ConfManager interface {
	GetDBConf() dbConf
	// 获取 源表和目标表 的映射配置，参考 tableMatchConf 的定义
	GetTableMatchConf() tableMatchConf
	GetImpalaConf() impalaConf
	// 获取 源表名 和 目标表名 的映射关系，用于 count job 对比
	GetTableNameMap() map[string]string
	// 获取 目标表名 和 datalink 任务id 的对应关系，用于 recovery job 重启任务
	GetTableTaskMap() map[string]int
	// 用于触发配置更新
	Update() <-chan struct{}
	// 返回前调用
	start()
}

// conf class
type confClass struct {
	Name            string `yaml:"name"`
	Interval        int    `yaml:"interval"`
	MinMappingCount int    `yaml:"min_mapping_count"`
}

// 根据配置获取 config manager 实现类
func GetConfManager(configManager config.Manager) ConfManager {
	confManagerLock.Do(func() {
		confClass := new(confClass)
		configManager.Unmarshal(spaceConfClass, confClass)
		switch confClass.Name {
		case confManagerClassFile:
			confManagerInstance = &fileConfManager{
				configManager: configManager,
			}
		case confManagerClassDatalink:
			confManagerInstance = &datalinkConfManager{
				configManager: configManager,
				classConf:     *confClass,
			}
		}
		confManagerInstance.start()
	})
	return confManagerInstance
}
