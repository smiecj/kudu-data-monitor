# kudu-data-monitor (kudu 实时数据监控)
需求背景: kudu 数据需要保持和源数据（mysql）完全一致

## 使用方式
### 基本配置
#### mysql 配置
参考 config.yml，在 db 域内 填写 mysql 的基本配置即可
db:
  db_info:
    - db: 数据库介质名称（注意不是库名，需要唯一标识，一般为 db_ip_库名）
      host: 数据库域名或ip
      port: 端口
      user: 账号，一般为只读账号 canal
      password: 账号密码
      database: 数据库名

#### impala 地址
参考 config.yml，在 impala 域内 填写 impala 的地址
impala:
  host: impalad 域名或ip
  port: impalad 端口
  database: 库名，当前数据架构下，实时数据都放在同一张表中

#### 监控周期
需要根据实际任务执行情况，确认任务执行周期 和 单次超时时间，比如 在线上跑监控任务，需要查询的表比较多，数据量也比较多，impala 查询 count 可能会比较费时，需要将周期尽量设置长一些

job:
  interval: 600 （执行周期，单位秒）
  timeout: 300 （超时时间，单位秒，必须小于周期）
  env: test (环境名, 在告警标题中体现)

#### 告警发送
目前支持通过QQ邮件发送告警，需要提供QQ邮箱的 SMTP token、发送人和接收人，多个接收人通过逗号分隔
mail:
  sender: xxx
  token: xxx
  receiver: xxx

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
./main --conf=/opt/modules/kudu-data-monitor/config.yml

## 环境依赖

- 占用一个端口提供 prometheus 监控接口

## 后续完善
### 任务执行超时，或者是 执行失败，可以包含更多的错误信息
-- 可通过开一个新的 channel 单独进行错误信息的传递

### 数据量对比阈值放到配置文件中
-- 参考: pkg/monitorjob/countmonitorjob.go

### 数据量统计周报（已实现）
每周发送一次 所有表的数据量对比