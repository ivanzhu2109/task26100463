# rdpadmit

进程内远程桌面会话准入组件（Go 库）：只负责排队、握手占位与会话释放，
不实现工作站管理。状态保存在内存中。

## 规则

- 站点及容量在构造时固定；每个用户跨所有站点最多占用 3 份额度。
- 握手中 / 已建立的请求同时占用一份用户额度和一份站点额度；排队请求不占额度。
- 每站点按 `Open` 的线性化顺序 FIFO 分配额度；队首因用户额度不足等待时，
  后续请求不得插队；多个站点争用同一用户额度时任一合法选择均可。
- 排队满 10 分钟（600000ms）终结；获得额度后握手满 15 秒（15000ms）终结；
  截止时刻本身属于到期；已建立会话不自动到期。
- 所有超时只依据 `Advance` 推进的逻辑毫秒时间，组件不使用真实时钟。
- Broker 调用在独立 goroutine 中进行：`Open`/`Cancel`/`Advance`/`View`
  永不阻塞在 Broker 上；迟到的成功句柄会被恰好关闭一次。

## 使用

```go
broker := myBroker{} // 实现 rdpadmit.Broker
m := rdpadmit.New(map[string]int{"site-a": 4, "site-b": 2}, broker)

m.Open("req-1", "user-1", "site-a") // 立即返回，不等待握手
m.Open("req-1", "user-1", "site-a") // 重复提交：对应同一请求

st := m.View("req-1")               // StateQueued / StateHandshaking / StateEstablished / ...

m.Cancel("req-1")                   // 取消排队/握手，或结束已建立的会话

m.Advance(nowMillis)                // 推进单调递增的逻辑时间，处理到期请求
```

`Broker` 接口：

```go
type Broker interface {
    Open(key string) (Handle, error) // 最终返回唯一会话句柄或失败，可能阻塞
    Close(handle Handle)             // 关闭句柄，保证成功，可能阻塞
}
```

## 测试

```sh
go test ./...
```

测试使用可控 Broker（`helper_test.go`）与逻辑时间交错验证：
并发重复提交与取消/成功的两种顺序、队首阻塞与跨站点额度释放、
15 秒截止点超时后的迟到成功句柄处理、排队超时与握手失败。
