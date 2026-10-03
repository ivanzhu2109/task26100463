# rdsession

进程内远程桌面会话准入组件（Go 库）。负责排队、握手占位与会话释放；
不管工作站，状态全在内存。站点及容量构造时固定，每用户跨站点最多 3 份额度。

## 使用

```go
// 实现调用方的进程内握手 Broker（Open/Close 均可长时间阻塞，Close 保证成功）。
type Broker interface {
	Open(key string) (Handle, error)
	Close(handle Handle)
}

m := rdsession.New(map[string]int{"site-a": 10, "site-b": 4}, myBroker)

m.Open("req-1", "alice", "site-a") // 立即返回；同 key 重复/并发提交幂等
st := m.View("req-1")              // Queued / Handshaking / Established / ...
m.Cancel("req-1")                  // 取消排队/握手，或结束已建立会话；可重复调用
m.Advance(nowMillis)               // 推进单调逻辑毫秒时间（从 0 开始），驱动超时
```

## 语义要点

- 排队满 10 分钟、握手满 15 秒即终结，截止时刻本身算到期；已建立会话不自动到期。
- 握手中/已建立的请求同时占一份用户额度和一份站点额度；排队不占额度。
- 每站点按 Open 线性化顺序 FIFO；队首因用户额度不足等待时不允许插队。
- 请求终结后永不重新入场；迟到成功的句柄会被自动关闭且只关一次。
- 组件内部并发发起 Broker 调用，Open/Cancel/Advance 均不等待 Broker。

## 测试

```sh
go test ./...        # 或 CGO_ENABLED=1 go test -race ./...
```

测试使用可控 Broker 与逻辑时间，无真实睡眠依赖。
