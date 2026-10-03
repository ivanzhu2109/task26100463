package rdpadmit

import (
	"errors"
	"sync"
	"testing"
)

// 场景 1a：并发重复提交同一 key；先取消，握手随后成功。
// 验证只发起一次握手、额度无泄漏、句柄只关闭一次、终结后不重新入场。
func TestConcurrentDuplicateOpen_CancelBeforeSuccess(t *testing.T) {
	b := newTestBroker()
	m := New(map[string]int{"s1": 1}, b)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Open("k1", "u1", "s1")
		}()
	}
	call := awaitOpen(t, b)
	if call.key != "k1" {
		t.Fatalf("Broker.Open key = %q, want k1", call.key)
	}
	wg.Wait()
	if n := b.openCount("k1"); n != 1 {
		t.Fatalf("Broker.Open(k1) called %d times, want 1", n)
	}

	// 先取消，握手随后才成功：迟到句柄必须被关闭。
	m.Cancel("k1")
	if got := m.View("k1"); got != StateCancelled {
		t.Fatalf("state = %v, want Cancelled", got)
	}
	call.res <- openResult{handle: "h1"}
	if h := awaitClose(t, b); h != "h1" {
		t.Fatalf("closed handle = %v, want h1", h)
	}
	noClose(t, b) // 句柄只关闭一次

	// 额度无泄漏：同用户同站点的新请求可以立即开始握手。
	m.Open("k2", "u1", "s1")
	if call2 := awaitOpen(t, b); call2.key != "k2" {
		t.Fatalf("Broker.Open key = %q, want k2", call2.key)
	}

	// 已终结的请求重复提交不重新入场。
	m.Open("k1", "u1", "s1")
	if got := m.View("k1"); got != StateCancelled {
		t.Fatalf("state = %v, want Cancelled", got)
	}
	noOpen(t, b)
}

// 场景 1b：并发重复提交同一 key；握手先成功，随后取消。
func TestConcurrentDuplicateOpen_SuccessBeforeCancel(t *testing.T) {
	b := newTestBroker()
	m := New(map[string]int{"s1": 1}, b)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Open("k1", "u1", "s1")
		}()
	}
	call := awaitOpen(t, b)
	wg.Wait()
	if n := b.openCount("k1"); n != 1 {
		t.Fatalf("Broker.Open(k1) called %d times, want 1", n)
	}

	call.res <- openResult{handle: "h1"}
	waitState(t, m, "k1", StateEstablished)

	m.Cancel("k1")
	if got := m.View("k1"); got != StateCancelled {
		t.Fatalf("state = %v, want Cancelled", got)
	}
	if h := awaitClose(t, b); h != "h1" {
		t.Fatalf("closed handle = %v, want h1", h)
	}
	m.Cancel("k1") // 重复取消无额外效果
	noClose(t, b)

	// 额度无泄漏。
	m.Open("k2", "u1", "s1")
	if call2 := awaitOpen(t, b); call2.key != "k2" {
		t.Fatalf("Broker.Open key = %q, want k2", call2.key)
	}
}

// 场景 2：用户在另一站点占满 3 份，本站点队首阻塞且不允许插队；
// 释放另一站点额度后，队首无需再次提交即开始握手。
func TestHeadOfLineBlocking(t *testing.T) {
	b := newTestBroker()
	m := New(map[string]int{"sA": 3, "sB": 1}, b)

	// u1 在 sA 占满 3 份额度。
	for _, key := range []string{"a1", "a2", "a3"} {
		m.Open(key, "u1", "sA")
	}
	for range []string{"a1", "a2", "a3"} {
		call := awaitOpen(t, b)
		call.res <- openResult{handle: "h-" + call.key}
	}
	for _, key := range []string{"a1", "a2", "a3"} {
		waitState(t, m, key, StateEstablished)
	}

	// u1 在 sB 的队首因用户额度不足而等待。
	m.Open("b1", "u1", "sB")
	noOpen(t, b)
	if got := m.View("b1"); got != StateQueued {
		t.Fatalf("b1 state = %v, want Queued", got)
	}

	// 后来的其他用户不得插队。
	m.Open("b2", "u2", "sB")
	noOpen(t, b)
	if got := m.View("b2"); got != StateQueued {
		t.Fatalf("b2 state = %v, want Queued", got)
	}

	// 释放 sA 的一份额度后，b1 无需再次提交即开始握手。
	m.Cancel("a1")
	if h := awaitClose(t, b); h != "h-a1" {
		t.Fatalf("closed handle = %v, want h-a1", h)
	}
	call := awaitOpen(t, b)
	if call.key != "b1" {
		t.Fatalf("Broker.Open key = %q, want b1", call.key)
	}

	// sB 容量为 1，b2 仍在排队。
	noOpen(t, b)
	if got := m.View("b2"); got != StateQueued {
		t.Fatalf("b2 state = %v, want Queued", got)
	}

	// b1 结束后 b2 才能入场。
	call.res <- openResult{handle: "h-b1"}
	waitState(t, m, "b1", StateEstablished)
	m.Cancel("b1")
	if h := awaitClose(t, b); h != "h-b1" {
		t.Fatalf("closed handle = %v, want h-b1", h)
	}
	if call := awaitOpen(t, b); call.key != "b2" {
		t.Fatalf("Broker.Open key = %q, want b2", call.key)
	}
}

// 场景 3：握手恰在 15 秒截止点超时，空位被新请求占用，旧握手随后成功。
// 验证旧句柄被关闭、新请求额度未被误释放、阻塞旧 Close 不妨碍他人。
func TestHandshakeTimeoutLateSuccess(t *testing.T) {
	b := newTestBroker()
	m := New(map[string]int{"s1": 1}, b)

	m.Open("k1", "u1", "s1")
	call1 := awaitOpen(t, b) // t=0 获得额度开始握手

	// 恰在 15 秒截止点：截止时刻本身属于到期。
	m.Advance(15000)
	if got := m.View("k1"); got != StateExpired {
		t.Fatalf("k1 state = %v, want Expired", got)
	}

	// 空位被新请求占用。
	m.Open("k2", "u2", "s1")
	call2 := awaitOpen(t, b)
	if call2.key != "k2" {
		t.Fatalf("Broker.Open key = %q, want k2", call2.key)
	}
	// k3 排队，用于探测额度是否被误释放。
	m.Open("k3", "u3", "s1")

	// 旧握手随后成功：句柄必须被关闭；先阻塞该 Close。
	gate := b.blockClose("h1")
	call1.res <- openResult{handle: "h1"}
	noClose(t, b) // Close(h1) 被闸门阻塞

	// 旧请求的迟到成功不得误释放 k2 的额度：k3 不能入场。
	noOpen(t, b)
	if got := m.View("k3"); got != StateQueued {
		t.Fatalf("k3 state = %v, want Queued", got)
	}

	// 阻塞旧 Close 时，其他请求仍可推进。
	call2.res <- openResult{handle: "h2"}
	waitState(t, m, "k2", StateEstablished)
	m.Cancel("k2")
	if h := awaitClose(t, b); h != "h2" {
		t.Fatalf("closed handle = %v, want h2", h)
	}
	// k2 释放额度后 k3 入场，依旧不受阻塞的 Close(h1) 影响。
	call3 := awaitOpen(t, b)
	if call3.key != "k3" {
		t.Fatalf("Broker.Open key = %q, want k3", call3.key)
	}

	// 打开闸门：旧句柄被关闭且只关闭一次。
	close(gate)
	if h := awaitClose(t, b); h != "h1" {
		t.Fatalf("closed handle = %v, want h1", h)
	}
	noClose(t, b)

	call3.res <- openResult{handle: "h3"}
	waitState(t, m, "k3", StateEstablished)
}

// 排队满 10 分钟终结，截止时刻本身属于到期；已建立会话不自动到期。
func TestQueueTimeout(t *testing.T) {
	b := newTestBroker()
	m := New(map[string]int{"s1": 1}, b)

	m.Open("k1", "u1", "s1")
	call := awaitOpen(t, b)
	call.res <- openResult{handle: "h1"}
	waitState(t, m, "k1", StateEstablished)

	m.Open("k2", "u2", "s1") // 站点满，排队
	m.Advance(599_999)
	if got := m.View("k2"); got != StateQueued {
		t.Fatalf("k2 state = %v, want Queued", got)
	}
	m.Advance(600_000) // 恰好 10 分钟
	if got := m.View("k2"); got != StateExpired {
		t.Fatalf("k2 state = %v, want Expired", got)
	}
	// 已建立会话不自动到期。
	m.Advance(10_000_000)
	if got := m.View("k1"); got != StateEstablished {
		t.Fatalf("k1 state = %v, want Established", got)
	}
}

// Broker.Open 失败：请求终结并归还额度，后续请求得以推进。
func TestHandshakeFailure(t *testing.T) {
	b := newTestBroker()
	m := New(map[string]int{"s1": 1}, b)

	m.Open("k1", "u1", "s1")
	call := awaitOpen(t, b)
	m.Open("k2", "u2", "s1") // 排队
	call.res <- openResult{err: errors.New("refused")}
	waitState(t, m, "k1", StateFailed)
	if call2 := awaitOpen(t, b); call2.key != "k2" {
		t.Fatalf("Broker.Open key = %q, want k2", call2.key)
	}
}
