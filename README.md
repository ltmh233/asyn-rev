# asyn-rev

基于 **Go + Redis Streams** 的分布式异步任务执行系统（个人学习与工程实践项目）。

> 当前是用于探索任务可靠性与故障恢复的原型实现，**未提供生产级 SLA、压测结论或完整监控体系**。

## 技术设计

- **Server**：接收任务创建请求，查询任务状态；使用 Redis Hash 存储任务元数据。
- **Worker**：通过 Redis Streams Consumer Group 消费任务，发送心跳；检查 Pending 消息并通过 `XCLAIM` 尝试接管异常 Worker 的任务。
- **Scheduler**：扫描 Redis ZSet 延迟队列，将到期重试任务重新投递到 Stream。
- **状态变更**：通过 Redis Lua 脚本组合状态更新、消息 ACK 及重试入队操作，以减少中间状态不一致窗口。

采用 **at-least-once** 交付语义：发生故障时任务可能重新执行，使用方必须自行保证业务幂等。这里的任务类型包括 `sleep` 测试处理器；`email` 和 `image` 目前为示例占位实现，并非完整业务服务。

## 项目结构

```text
cmd/
  server/         # HTTP 接入
  worker/         # 消费、执行与接管
  sheduler/       # 延迟重试调度器（当前目录名）
internal/
  queue/          # Redis Streams 封装
  registry/       # Worker 心跳和存活判定
  retryqueue/     # 延迟重试队列
  transition/     # Redis Lua 状态迁移
  taskstore/      # 任务元数据持久化
```

## 本地运行

要求：Go（以 `go.mod` 为准）与 Redis。请**仅使用本机或隔离测试环境**；不要直接把示例服务暴露在公网。

1. 启动 Redis，按需修改根目录 `config.yaml` 中的 Redis 和 HTTP 服务地址。**不要在配置中提交真实密码**。
2. 在仓库根目录分别启动三个进程：

```bash
go run ./cmd/server
go run ./cmd/sheduler
go run ./cmd/worker -id 1
```

3. 创建并查看测试任务：

```bash
curl -X POST http://127.0.0.1:8080/createtask \
  -H 'Content-Type: application/json' \
  -d '{"type":"sleep","payload":{"if_failed":0,"seconds":2}}'

# 将返回值中的 task_id 替换到下面的 URL
curl 'http://127.0.0.1:8080/gettask?id=1'
```

仓库另有 `api.http` 便于使用支持 HTTP Client 的编辑器调试。命令中的地址/端口应以你实际的 `config.yaml` 为准。

## 重点问题与当前限制

- **重复执行**：Worker 执行成功但还未 ACK 时崩溃，存在再次执行的可能；系统不承诺 exactly-once。
- **失败重试**：使用 ZSet 和指数退避；请根据任务副作用设计幂等键和重试上限。
- **测试与性能**：本仓库目前未附公开可复现的压测报告，因此不宣称具体吞吐、恢复时间或高可用指标。
- **部署与安全**：缺少生产环境必需的鉴权、隔离、监控及配额防护；不要直接对外提供服务。

## 状态

个人项目，持续完善中。欢迎通过 Issue 讨论代码设计与边界条件。
