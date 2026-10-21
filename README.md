# download

纯 Go 并发下载库：Range 分块 + 全局 worker 池 + 断点续传 + 任务管理与进程内事件流。

包边界止于 `<-chan download.Event`。不含 HTTP 服务、SSE/WebSocket/gRPC 端点、鉴权、监听地址或任何线上协议序列化；这些由使用方在 `Subscribe` 之上自建。

```go
import "github.com/xiaoLangZe/go_muyuan"
```

## 快速开始

```go
r, err := download.New(
	download.WithThreads(8),                    // 全局下载槽位
	download.WithRootDir("/srv/downloads"),     // 路径围栏（默认 strict）
	download.WithProgressInterval(300*time.Millisecond),
)
if err != nil {
	log.Fatal(err)
}
defer r.Close()

events, cancel := r.Subscribe()
defer cancel()

// 先订阅、后快照：Snapshot 的 Seq 是分界线。
base, err := r.Snapshot(download.WithSnapshotDetails(false))
if err != nil {
	log.Fatal(err)
}
go func() {
	for ev := range events {
		if ev.Seq <= base.Seq {
			continue
		}
		switch ev.Type {
		case download.EventTaskState:
			info := ev.Payload.(download.TaskInfo)   // 增/改本地视图
		case download.EventTaskRemoved:
			// 删本地视图
		case download.EventTaskProgress:
			p := ev.Payload.(download.Progress)       // 覆盖进度字段
		case download.EventTaskAdded:
			info := ev.Payload.(download.TaskInfo)
		}
	}
}()

id, err := r.Add("https://example.com/file.zip", "media") // saveDir 是目录
if err != nil {
	log.Fatal(err)
}
if err := r.Wait(ctx, id); err != nil { // ctx 只控制等待
	log.Printf("download failed: %v", err)
}
```

`Add` 只做本地校验与登记，立即返回 `(TaskID, error)`；404、磁盘满、断网等运行期失败通过 `Get`、`List`、`Snapshot` 和事件观察，`Wait` 会返回任务错误。

## 语义要点

- **线程模型**：每个任务一个协调协程（探测、分块、收尾），不占下载槽位；所有切片进入同一个全局队列，worker 混合消费。`Threads` 是整个 Role 的总并发。
- **两套 ctx**：调用方 ctx 只控制 `Wait`/`Download` 的等待，取消它不会停止任务；停止任务用 `Pause`/`Cancel`/`Remove`，句柄是 `TaskID`。
- **断点续传**：sidecar 为 `<目标文件>.dltmp.json`，记录分块位图与 ETag/Last-Modified。恢复时先探测，再用 `If-Range` 校验；远端文件变化会导致整次重下，而不是拼接两个版本。目标文件丢失时 sidecar 自动作废。
- **事件**：`task_added`、`task_state`、`task_removed`、`worker_change` 不可丢；订阅者缓冲满时会被踢下线并关闭 channel。`task_progress` 是采样事件，可合并，同一任务的进度以最新值为准。
- **序列号**：`Event.Seq` 全局递增，可用于检测缺口（缺口即重新 `Snapshot` 对齐，不修补）和配合 `WithSince` 续订。SSE 把 `Seq` 映射为 `id:`、把 `Last-Event-ID` 映射为 `WithSince(seq)` 即可重连。
- **安全默认**：仅允许 http/https；默认拒绝 localhost、环回、私有、链路本地与保留地址（字面量与 DNS 解析结果都会检查，重定向同样生效）。内网下载、本地测试服务或本地代理需显式 `WithAllowPrivateHosts(true)`。
- **路径防护**：解析为绝对路径；`WithRootDir` 下相对 `saveDir` 永远受限于 root；绝对 `saveDir` 默认也必须在 root 内（`WithStrictRoot(false)` 放宽）；现有符号链接会被拒绝。
- **去重**：同一解析后路径只允许一个活跃任务。默认返回 `*DuplicateTaskError`（含已有 ID）；`WithIdempotent(true)` 返回已有 ID 和 `nil`。已完成后路径锁释放，此时按 `WithOverwritePolicy` 处理已存在的文件。

## API

```go
New(opts ...Option) (*Role, error)

Add(url, saveDir) (TaskID, error)
AddWithOptions(url, saveDir, opts ...TaskOption) (TaskID, error)
Download(ctx, url, saveDir) error          // Add + Wait
Wait(ctx, id) error

Pause(id) / Resume(id) / Cancel(id) error
Remove(id, cleanup bool) error
Update(id, TaskPatch) error

Get(id) (TaskInfo, bool)
ListAll() []TaskInfo
List(ListFilter) ([]TaskInfo, error)
Stats() RoleStats
Snapshot(opts ...SnapshotOption) (StatusSnapshot, error)

AddWorker(n) / RemoveWorker(n) / SetThreads(n) / NumWorkers()

Subscribe(opts ...SubscribeOption) (<-chan Event, func())
Close() error
```

`New` 返回 error 是唯一与设计清单的签名差异：代理 URL 解析、`rootDir` 可写性等构造期校验必须能上报。

### 角色选项

| 选项 | 说明 |
|---|---|
| `WithThreads(n)` | 全局下载槽位，默认 4，必须 ≥ 1 |
| `WithUserAgent(ua)` | 所有请求统一注入 UA，任务级可覆盖 |
| `WithProxy(url)` / `WithProxyFromEnvironment()` | 代理；二者互斥，构造期校验 |
| `WithRootDir(dir)` / `WithStrictRoot(bool)` | 路径围栏与绝对路径严格模式（默认严格） |
| `WithMaxRetries(n)` / `WithRetryBase(d)` | 重试次数与指数退避基准，默认 3 / 500ms |
| `WithGlobalRateLimit(bytesPerSec)` | 全局限速，0 = 不限 |
| `WithResume(bool)` | 任务默认是否启用续传，默认 true |
| `WithMaxConcurrentTasks(n)` | 同时下载的文件数上限，0 = 不限 |
| `WithReplayBuffer(n)` | 事件回放缓冲，默认 0（关闭） |
| `WithProgressInterval(d)` | 进度采样间隔，默认 500ms |
| `WithAllowPrivateHosts(bool)` | 允许私有/环回地址，默认 false |
| `WithHTTPClient(hc)` | 使用自定义 client（仍注入 UA，尽量保留私有地址防护） |

### 任务选项

`WithFileName`、`WithPriority`、`WithTaskRateLimit`、`WithTaskResume`、`WithTaskUserAgent`、`WithTags`、`WithIdempotent`、`WithOverwritePolicy`。

`OverwritePolicy`：`OverwriteError`（默认）、`OverwriteSkip`（同样返回 `ErrFileExists`，便于调用方视为“已存在”）、`OverwriteReplace`、`OverwriteResume`。

### 过滤、排序与分页

`ListFilter` 零值匹配全部；跨字段 AND、同字段 OR。支持状态/ID、URL/路径（exact/prefix/contains）、创建与更新时间区间、优先级、tag 子集、`OnlyFailed`/`HasError`，以及 `SortBy`（created_at、updated_at、priority、progress、speed、status）、`SortDesc`、`Offset`、`Limit`。执行顺序固定为匹配 → 排序 → 分页；排序稳定，同值按 ID。`speed` 为 0 的未知值始终排在最后。非法值使 `List` 返回 `ErrInvalidFilter`。

### 订阅选项

`WithEventTypes`、`WithSubscribeTasks`、`WithSubscribeBuffer`、`WithSubscribeInterval`、`WithSince`。`WithSince` 需要开启 `WithReplayBuffer`；序列过旧时 channel 立即关闭，调用方应回退到 `Snapshot`。

## 文件结构

```
role.go        Role、选项、构造、Close、任务生命周期
executor.go    全局 worker 池、并发任务准入、动态伸缩
transport.go   UA、代理、HTTP 客户端、私有地址防护
path.go        路径解析、穿越与符号链接防护
registry.go    任务注册表、路径去重
filter.go      ListFilter、匹配、排序、分页
task.go        Task、状态机、TaskInfo
add.go         Add / AddWithOptions / TaskOption
download.go    协调协程、分块并发、续传、单线程降级
probe.go       能力探测
chunk.go       分块规划
meta.go        sidecar
retry.go       退避与重试
ratelimit.go   共享令牌桶
progress.go    采样、速度/ETA、Snapshot/Stats
hub.go         事件总线、序列号、背压、回放
subscribe.go   Subscribe、Event、SubscribeOption
```

## 测试

```bash
go test ./... -count=1
```

测试使用 `httptest` 覆盖 Range/无 Range、暂停恢复、取消后恢复、远端文件变化、sidecar 与目标文件不一致、404、路径穿越、去重/幂等、覆盖策略、事件顺序与回放、慢订阅者踢出、Close、限速器与状态机。
