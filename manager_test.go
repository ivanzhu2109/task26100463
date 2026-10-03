package rdsession

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// openCall 代表一次到达 Broker 的握手调用，测试通过 res 控制其返回。
type openCall struct {
	key string
	res chan openResult
}

type openResult struct {
	h   string
	err error
}

// testBroker 是可控 Broker：Open 阻塞直到测试应答，Close 记录并可被闸门阻塞。
type testBroker struct {
	opens  chan *openCall
	closes chan string

	mu        sync.Mutex
	openCount int
	closeGate chan struct{} // 非 nil 时 Close 阻塞直到闸门关闭
}

func newTestBroker() *testBroker {
	return &testBroker{
		opens:  make(chan *openCall, 128),
		closes: make(chan string, 128),
	}
}

func (b *testBroker) Open(key string) (Handle, error) {
	b.mu.Lock()
	b.openCount++
	b.mu.Unlock()
	c := &openCall{key: key, res: make(chan openResult, 1)}
	b.opens <- c
	r := <-c.res
	if r.err != nil {
		return nil, r.err
	}
	return r.h, nil
}

func (b *testBroker) Close(h Handle) {
	b.mu.Lock()
	gate := b.closeGate
	b.mu.Unlock()
	if gate != nil {
		<-gate
	}
	b.closes <- h.(string)
}

func (b *testBroker) numOpens() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.openCount
}

func (b *testBroker) blockClose() (unblock func()) {
	gate := make(chan struct{})
	b.mu.Lock()
	b.closeGate = gate
	b.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

func expectOpen(t *testing.T, b *testBroker) *openCall {
	t.Helper()
	select {
	case c := <-b.opens:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("expected a broker Open call, got none")
		return nil
	}
}

func expectNoOpen(t *testing.T, b *testBroker) {
	t.Helper()
	select {
	case c := <-b.opens:
		t.Fatalf("unexpected broker Open call for key %q", c.key)
	case <-time.After(50 * time.Millisecond):
	}
}

func expectClose(t *testing.T, b *testBroker) string {
	t.Helper()
	select {
	case h := <-b.closes:
		return h
	case <-time.After(2 * time.Second):
		t.Fatal("expected a broker Close call, got none")
		return ""
	}
}

func expectNoClose(t *testing.T, b *testBroker) {
	t.Helper()
	select {
	case h := <-b.closes:
		t.Fatalf("unexpected broker Close call for handle %q", h)
	case <-time.After(50 * time.Millisecond):
	}
}

func waitStatus(t *testing.T, m *Manager, key string, want Status) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := m.View(key); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("key %q: status = %v, want %v", key, m.View(key), want)
}

// 并发重复提交同一 key：只发起一次握手；取消与成功两种顺序下额度不泄漏、句柄只关一次。
func TestConcurrentDuplicateOpen(t *testing.T) {
	openConcurrently := func(m *Manager, key, user, site string) {
		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.Open(key, user, site)
			}()
		}
		wg.Wait()
	}

	t.Run("SuccessThenCancel", func(t *testing.T) {
		b := newTestBroker()
		m := New(map[string]int{"s": 1}, b)
		openConcurrently(m, "k", "u", "s")
		c := expectOpen(t, b)
		expectNoOpen(t, b)
		if n := b.numOpens(); n != 1 {
			t.Fatalf("broker Open called %d times, want 1", n)
		}
		c.res <- openResult{h: "h1"}
		waitStatus(t, m, "k", StatusEstablished)
		m.Cancel("k")
		waitStatus(t, m, "k", StatusClosed)
		if h := expectClose(t, b); h != "h1" {
			t.Fatalf("closed handle %q, want h1", h)
		}
		expectNoClose(t, b)
		// 重复提交已终结的请求：无效果。
		openConcurrently(m, "k", "u", "s")
		expectNoOpen(t, b)
		// 额度无泄漏：新请求可以立即开始握手。
		m.Open("k2", "u", "s")
		expectOpen(t, b)
	})

	t.Run("CancelThenLateSuccess", func(t *testing.T) {
		b := newTestBroker()
		m := New(map[string]int{"s": 1}, b)
		openConcurrently(m, "k", "u", "s")
		c := expectOpen(t, b)
		expectNoOpen(t, b)
		m.Cancel("k")
		m.Cancel("k") // 重复取消无额外效果
		waitStatus(t, m, "k", StatusCancelled)
		// 握手在请求终结后才成功：迟到句柄必须被关闭且只关一次。
		c.res <- openResult{h: "h-late"}
		if h := expectClose(t, b); h != "h-late" {
			t.Fatalf("closed handle %q, want h-late", h)
		}
		expectNoClose(t, b)
		// 额度无泄漏：同用户同站点的新请求可以立即开始握手。
		m.Open("k2", "u", "s")
		c2 := expectOpen(t, b)
		c2.res <- openResult{h: "h2"}
		waitStatus(t, m, "k2", StatusEstablished)
	})
}

// 用户额度被他站占满时本站队首阻塞，后来用户不得插队；释放他站额度后队首自动开始握手。
func TestHeadOfLineBlockingAcrossSites(t *testing.T) {
	b := newTestBroker()
	m := New(map[string]int{"a": 3, "b": 1}, b)

	// alice 在站点 a 占满 3 份用户额度。
	calls := make(map[string]*openCall)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("a%d", i)
		m.Open(key, "alice", "a")
		c := expectOpen(t, b)
		calls[key] = c
		c.res <- openResult{h: "h-" + key}
		waitStatus(t, m, key, StatusEstablished)
	}

	// alice 在站点 b 排队（用户额度不足），成为队首。
	m.Open("b0", "alice", "b")
	waitStatus(t, m, "b0", StatusQueued)
	// bob 排在后面：即使他有用户额度且站点 b 有空位，也不得绕过队首。
	m.Open("b1", "bob", "b")
	waitStatus(t, m, "b1", StatusQueued)
	expectNoOpen(t, b)

	// 释放站点 a 的一份额度：队首 b0 无需再次提交便开始握手。
	m.Cancel("a0")
	waitStatus(t, m, "a0", StatusClosed)
	c := expectOpen(t, b)
	if c.key != "b0" {
		t.Fatalf("handshake started for %q, want b0", c.key)
	}
	waitStatus(t, m, "b0", StatusHandshaking)
	// 站点 b 容量为 1，bob 继续排队。
	waitStatus(t, m, "b1", StatusQueued)
	expectNoOpen(t, b)
}

// 握手恰在 15 秒截止点超时；空位被新请求占用后旧握手迟到成功：
// 旧句柄被关闭、新请求额度不受影响；阻塞旧 Close 时其他请求仍可推进。
func TestHandshakeTimeoutAtDeadline(t *testing.T) {
	b := newTestBroker()
	m := New(map[string]int{"s": 1}, b)

	m.Open("k1", "u1", "s")
	c1 := expectOpen(t, b)
	waitStatus(t, m, "k1", StatusHandshaking)

	// 截止时刻本身属于到期。
	m.Advance(15000)
	waitStatus(t, m, "k1", StatusTimedOut)

	// 空位被新请求占用。
	m.Open("k2", "u2", "s")
	c2 := expectOpen(t, b)
	c2.res <- openResult{h: "h2"}
	waitStatus(t, m, "k2", StatusEstablished)

	// 旧握手随后成功：句柄被关闭，且不影响 k2。
	c1.res <- openResult{h: "h1"}
	if h := expectClose(t, b); h != "h1" {
		t.Fatalf("closed handle %q, want h1", h)
	}
	waitStatus(t, m, "k2", StatusEstablished)
	expectNoOpen(t, b)

	// 阻塞后续 Close：其他请求必须仍可推进。
	unblock := b.blockClose()
	defer unblock()
	m.Cancel("k2") // h2 的 Close 被闸门阻塞
	waitStatus(t, m, "k2", StatusClosed)
	m.Open("k3", "u3", "s")
	c3 := expectOpen(t, b) // 额度已同步释放，k3 立即开始握手
	c3.res <- openResult{h: "h3"}
	waitStatus(t, m, "k3", StatusEstablished)
	unblock()
	if h := expectClose(t, b); h != "h2" {
		t.Fatalf("closed handle %q, want h2", h)
	}
	expectNoClose(t, b)
}

// 排队满 10 分钟终结，截止时刻本身属于到期。
func TestQueueTimeout(t *testing.T) {
	b := newTestBroker()
	m := New(map[string]int{"s": 3}, b)

	// 占满用户额度，使后续请求排队。
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("e%d", i)
		m.Open(key, "u", "s")
		c := expectOpen(t, b)
		c.res <- openResult{h: "h-" + key}
		waitStatus(t, m, key, StatusEstablished)
	}
	m.Open("q", "u", "s")
	waitStatus(t, m, "q", StatusQueued)

	m.Advance(600000 - 1)
	waitStatus(t, m, "q", StatusQueued)
	m.Advance(600000)
	waitStatus(t, m, "q", StatusTimedOut)
	// 终结后永不重新入场。
	m.Cancel("e0")
	waitStatus(t, m, "e0", StatusClosed)
	expectNoOpen(t, b)
}

// 握手失败：请求终结、额度归还，后续排队请求被推动。
func TestHandshakeFailure(t *testing.T) {
	b := newTestBroker()
	m := New(map[string]int{"s": 1}, b)

	m.Open("k1", "u1", "s")
	c1 := expectOpen(t, b)
	c1.res <- openResult{err: errors.New("refused")}
	waitStatus(t, m, "k1", StatusFailed)

	m.Open("k2", "u2", "s")
	c2 := expectOpen(t, b)
	c2.res <- openResult{h: "h2"}
	waitStatus(t, m, "k2", StatusEstablished)
}
