// Package rdpadmit 实现进程内远程桌面会话准入：
// 排队、握手占位与会话释放。站点及容量在构造时固定，
// 每个用户跨所有站点最多占用 MaxUserShares 份额度。
//
// 组件只负责准入：每站点按 Open 的线性化顺序 FIFO 分配额度，
// 队首因用户额度不足等待时不允许后续请求绕过；握手与句柄关闭
// 由调用方提供的 Broker 承担，组件在独立 goroutine 中发起调用，
// Open、Cancel、Advance、View 均不会阻塞在 Broker 上。
package rdpadmit

import "sync"

const (
	// MaxUserShares 是单个用户跨所有站点的额度上限。
	MaxUserShares = 3
	// QueueTimeoutMs 是排队超时（10 分钟，逻辑毫秒）。
	QueueTimeoutMs = 600_000
	// HandshakeTimeoutMs 是获得额度后的握手超时（15 秒，逻辑毫秒）。
	HandshakeTimeoutMs = 15_000
)

// State 是请求的可观察状态。
type State int

const (
	// StateUnknown 表示 key 不存在。
	StateUnknown State = iota
	// StateQueued 表示请求在站点队列中等待额度，不占用任何额度。
	StateQueued
	// StateHandshaking 表示请求已占用用户与站点额度，握手进行中。
	StateHandshaking
	// StateEstablished 表示握手成功，会话已建立，仍占用两类额度。
	StateEstablished
	// StateCancelled 表示请求被 Cancel 终结。
	StateCancelled
	// StateFailed 表示 Broker.Open 返回失败。
	StateFailed
	// StateExpired 表示排队或握手超时。
	StateExpired
)

// terminal 报告状态是否为终结态。
func (s State) terminal() bool {
	return s == StateCancelled || s == StateFailed || s == StateExpired
}

// request 是单个 key 对应的内部记录。
type request struct {
	key        string
	user       string
	site       string
	state      State
	enqueuedAt int64 // 进入队列时的逻辑时间
	grantAt    int64 // 获得额度（开始握手）时的逻辑时间
	handle     Handle
}

// Manager 是准入组件，所有状态保存在内存中。
type Manager struct {
	mu       sync.Mutex
	broker   Broker
	now      int64
	siteCap  map[string]int
	siteUsed map[string]int
	userUsed map[string]int
	queues   map[string][]string // 站点 -> 排队中的 key，FIFO
	reqs     map[string]*request
}

// New 构造组件。sites 为站点名到容量的映射，构造后固定。
func New(sites map[string]int, broker Broker) *Manager {
	m := &Manager{
		broker:   broker,
		siteCap:  make(map[string]int, len(sites)),
		siteUsed: make(map[string]int, len(sites)),
		userUsed: make(map[string]int),
		queues:   make(map[string][]string, len(sites)),
		reqs:     make(map[string]*request),
	}
	for site, cap := range sites {
		m.siteCap[site] = cap
	}
	return m
}

// Open 提交请求并立即返回。同一 key 的重复或并发提交（包括请求
// 已终结之后）都对应同一请求，不会再次入场。
func (m *Manager) Open(key, user, site string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.reqs[key]; ok {
		return
	}
	m.reqs[key] = &request{
		key:        key,
		user:       user,
		site:       site,
		state:      StateQueued,
		enqueuedAt: m.now,
	}
	m.queues[site] = append(m.queues[site], key)
	m.scheduleLocked()
}

// Cancel 取消排队或握手中的请求，或结束已建立的会话；重复调用无额外效果。
func (m *Manager) Cancel(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.reqs[key]
	if !ok || r.state.terminal() {
		return
	}
	switch r.state {
	case StateQueued:
		m.dequeueLocked(r)
		r.state = StateCancelled
	case StateHandshaking:
		m.releaseLocked(r)
		r.state = StateCancelled
	case StateEstablished:
		m.releaseLocked(r)
		r.state = StateCancelled
		m.closeAsync(r)
	}
	m.scheduleLocked()
}

// View 读取请求当前状态；未知 key 返回 StateUnknown。
func (m *Manager) View(key string) State {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.reqs[key]; ok {
		return r.state
	}
	return StateUnknown
}

// Advance 推进单调递增的逻辑毫秒时间，处理到期请求。
// 截止时刻本身属于到期；到期请求终结后永不重新入场。
func (m *Manager) Advance(now int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if now > m.now {
		m.now = now
	}
	for _, r := range m.reqs {
		switch r.state {
		case StateQueued:
			if r.enqueuedAt+QueueTimeoutMs <= now {
				m.dequeueLocked(r)
				r.state = StateExpired
			}
		case StateHandshaking:
			if r.grantAt+HandshakeTimeoutMs <= now {
				m.releaseLocked(r)
				r.state = StateExpired
			}
		}
	}
	m.scheduleLocked()
}

// scheduleLocked 推动所有站点：在站点额度有空余且队首用户额度
// 充足时，按 FIFO 取出队首，同时占用一份用户额度和一份站点额度
// 并开始握手。队首用户额度不足时该站点停止推进（不允许插队）。
// 每次状态变化后调用，保证没有输入时也不会留下可推进的队首。
func (m *Manager) scheduleLocked() {
	for site, queue := range m.queues {
		for len(queue) > 0 && m.siteUsed[site] < m.siteCap[site] {
			r := m.reqs[queue[0]]
			if m.userUsed[r.user] >= MaxUserShares {
				break
			}
			queue = queue[1:]
			m.queues[site] = queue
			m.userUsed[r.user]++
			m.siteUsed[site]++
			r.state = StateHandshaking
			r.grantAt = m.now
			go m.handshake(r)
		}
	}
}

// handshake 在不持锁的 goroutine 中调用 Broker.Open。
// 返回时若请求已终结，迟到的成功句柄必须关闭且不影响其他请求。
func (m *Manager) handshake(r *request) {
	handle, err := m.broker.Open(r.key)
	m.mu.Lock()
	if r.state == StateHandshaking {
		if err == nil {
			r.state = StateEstablished
			r.handle = handle
			m.mu.Unlock()
			return
		}
		m.releaseLocked(r)
		r.state = StateFailed
		m.scheduleLocked()
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	if err == nil {
		// 请求已终结（取消或超时）：迟到句柄异步关闭，不阻塞他人。
		go m.broker.Close(handle)
	}
}

// releaseLocked 归还请求持有的两类额度；仅在请求确实持有额度时
// 调用，且状态随即转为终结态，保证只归还一次。
func (m *Manager) releaseLocked(r *request) {
	m.userUsed[r.user]--
	m.siteUsed[r.site]--
}

// dequeueLocked 将排队请求移出站点队列。
func (m *Manager) dequeueLocked(r *request) {
	queue := m.queues[r.site]
	for i, key := range queue {
		if key == r.key {
			m.queues[r.site] = append(queue[:i], queue[i+1:]...)
			return
		}
	}
}

// closeAsync 取出已建立会话的句柄并异步关闭，保证只关闭一次。
func (m *Manager) closeAsync(r *request) {
	handle := r.handle
	r.handle = nil
	go m.broker.Close(handle)
}
