package rdpadmit

import (
	"testing"
	"time"
)

// openResult 是测试发给 Broker.Open 的返回值。
type openResult struct {
	handle Handle
	err    error
}

// openCall 表示一次已发起的 Broker.Open，测试通过 res 控制其返回。
type openCall struct {
	key string
	res chan openResult
}

// testBroker 是可完全由测试控制时序的 Broker：
// Open 登记后阻塞，直到测试通过 awaitOpen 拿到 openCall 并发送结果；
// Close 可按句柄设置闸门阻塞，完成的关闭进入 closes 通道。
type testBroker struct {
	openCounts chan string // 每次 Open 调用记录 key（缓冲足够大）
	calls      chan openCall
	closes     chan Handle
	gates      map[Handle]chan struct{} // 句柄 -> 关闭闸门
}

func newTestBroker() *testBroker {
	return &testBroker{
		openCounts: make(chan string, 1024),
		calls:      make(chan openCall, 1024),
		closes:     make(chan Handle, 1024),
		gates:      make(map[Handle]chan struct{}),
	}
}

func (b *testBroker) Open(key string) (Handle, error) {
	b.openCounts <- key
	call := openCall{key: key, res: make(chan openResult, 1)}
	b.calls <- call
	r := <-call.res
	return r.handle, r.err
}

func (b *testBroker) Close(h Handle) {
	if gate, ok := b.gates[h]; ok {
		<-gate
	}
	b.closes <- h
}

// blockClose 让指定句柄的 Close 阻塞，直到返回的闸门通道被关闭。
func (b *testBroker) blockClose(h Handle) chan struct{} {
	gate := make(chan struct{})
	b.gates[h] = gate
	return gate
}

// awaitOpen 等待下一次 Broker.Open 调用，超时则失败。
func awaitOpen(t *testing.T, b *testBroker) openCall {
	t.Helper()
	select {
	case call := <-b.calls:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Broker.Open")
		return openCall{}
	}
}

// awaitClose 等待下一次 Broker.Close 完成，超时则失败。
func awaitClose(t *testing.T, b *testBroker) Handle {
	t.Helper()
	select {
	case h := <-b.closes:
		return h
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Broker.Close")
		return nil
	}
}

// openCount 统计指定 key 的 Broker.Open 调用次数（排空计数通道）。
func (b *testBroker) openCount(key string) int {
	counts := map[string]int{}
	for {
		select {
		case k := <-b.openCounts:
			counts[k]++
		default:
			return counts[key]
		}
	}
}

// noOpen 断言在短暂窗口内没有新的 Broker.Open 调用。
// 仅用于验证“不会发生”的负向性质，窗口只是等待组件内部
// goroutine 完成调度，逻辑时序全部由逻辑时间控制。
func noOpen(t *testing.T, b *testBroker) {
	t.Helper()
	select {
	case call := <-b.calls:
		t.Fatalf("unexpected Broker.Open for key %q", call.key)
	case <-time.After(100 * time.Millisecond):
	}
}

// noClose 断言在短暂窗口内没有 Broker.Close 完成。
func noClose(t *testing.T, b *testBroker) {
	t.Helper()
	select {
	case h := <-b.closes:
		t.Fatalf("unexpected Broker.Close for handle %v", h)
	case <-time.After(100 * time.Millisecond):
	}
}

// waitState 轮询直到 key 达到期望状态（等待组件内部 goroutine 推进）。
func waitState(t *testing.T, m *Manager, key string, want State) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := m.View(key); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("key %q did not reach state %v; got %v", key, want, m.View(key))
}
