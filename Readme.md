# kudu-data-monitor (kudu 实时数据监控)
需求背景: 通过 datalink 从 mysql 同步数据到 kudu , kudu 数据需要保持和源数据（mysql）完全一致，由于 源表进行 ddl 后 会导致 kudu 数据无法正常同步，因此需要借助其他服务，进行 表重建、任务重置和重启的操作

## 使用方式
### 基本配置

#### 表映射配置

```
conf_class:
  name: 表映射加载来源，可选 datalink / file, 建议使用 datalink
  interval: 配置更新周期
  min_mapping_count: 最少映射数，用于做配置检查，防止配置读取失败的时候误覆盖
```

#### impala 地址
```
impala:
  host: impalad 域名或ip
  port: impalad 端口
  database: 库名
```

#### datalink 服务地址
```
datalink:
  user: 服务登录用户名
  password: 服务登录密码
  host: 服务域名
  port: 服务端口
  session_internal: session id 更新周期
```

#### 统计数据差异任务配置

```
count:
  interval: 任务统计周期，建议1h 左右一次即可
  timeout: 单次统计任务的超时时间
```

#### datalink 任务恢复配置

参考 config.yml 中的配置详细描述

#### 监控周期
需要根据实际任务执行情况，确认任务执行周期 和 单次超时时间，比如 在线上跑监控任务，需要查询的表比较多，数据量也比较多，impala 查询 count 可能会比较费时，需要将周期尽量设置长一些

job:
  interval: 600 （执行周期，单位秒）
  timeout: 300 （超时时间，单位秒，必须小于周期）
  env: test (环境名, 在告警标题中体现)

#### 告警发送
mail:
  host: smtp host
  sender: 发送人邮箱
  token: smtp token
  receiver: 收件人邮箱

#### metrics 监控指标接口
monitor:
  port: http 服务端口

开启后，可通过 http://localhost:port/metrics 获取到具体指标

目前提供的指标: 
job_timecost_gauge: 每次任务执行耗时的记录

job_executed_count: 任务执行次数

### 部署方式
编译:
```
make build
```

启动:
./main --conf=./config.yml

## 环境依赖

- 占用一个端口提供 prometheus 监控接口

## 后续完善

### 数据量对比阈值放到配置文件中
-- 参考: pkg/monitorjob/countmonitorjob.go

### 数据量统计周报（已实现）
每周发送一次 所有表的数据量对比

### datalink 任务自动恢复（已实现）
通过 datalink 同步的 kudu 实时表，在发现和源表数据量有差别的时候，会自动重启任务