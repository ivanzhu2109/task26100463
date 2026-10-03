package rdsession

import "sync"

const (
	// queueTimeout 排队超时：10 分钟（逻辑毫秒）。
	queueTimeout = 10 * 60 * 1000
	// handshakeTimeout 握手超时：15 秒（逻辑毫秒）。
	handshakeTimeout = 15 * 1000
	// userLimit 每个用户跨所有站点的最大并发额度。
	userLimit = 3
)

// request 是一个准入请求的内部状态。
type request struct {
	key      string
	user     string
	site     string
	status   Status
	queuedAt int64 // 进入排队时的逻辑时间
	deadline int64 // 握手截止的逻辑时间
	handle   Handle
}

// siteState 保存单个站点的容量、已用额度与 FIFO 队列。
type siteState struct {
	capacity int
	used     int
	queue    []*request
}

// Manager 是进程内远程桌面会话准入组件。
// 站点及容量在构造时固定；每个用户跨所有站点最多占用 userLimit 份额度。
type Manager struct {
	mu     sync.Mutex
	broker Broker
	now    int64
	reqs   map[string]*request
	sites  map[string]*siteState
	users  map[string]int
}

// New 构造准入组件，站点及其容量在此固定。
func New(sites map[string]int, broker Broker) *Manager {
	m := &Manager{
		broker: broker,
		reqs:   make(map[string]*request),
		sites:  make(map[string]*siteState, len(sites)),
		users:  make(map[string]int),
	}
	for name, capacity := range sites {
		m.sites[name] = &siteState{capacity: capacity}
	}
	return m
}

// Open 提交请求并立即返回，不等待握手。
// 同一 key 总携带相同参数，重复或并发提交对应同一请求，包括已终结的请求。
func (m *Manager) Open(key, user, site string) {
	m.mu.Lock()
	if _, ok := m.reqs[key]; ok {
		m.mu.Unlock()
		return
	}
	s, ok := m.sites[site]
	if !ok {
		m.mu.Unlock()
		return
	}
	r := &request{key: key, user: user, site: site, status: StatusQueued, queuedAt: m.now}
	m.reqs[key] = r
	s.queue = append(s.queue, r)
	m.promoteLocked()
	m.mu.Unlock()
}

// Cancel 取消排队或握手中的请求，也用于结束已建立的会话。
// 重复调用或对未知 key 调用无额外效果。
func (m *Manager) Cancel(key string) {
	m.mu.Lock()
	r, ok := m.reqs[key]
	if !ok {
		m.mu.Unlock()
		return
	}
	var h Handle
	closeIt := false
	switch r.status {
	case StatusQueued:
		r.status = StatusCancelled
		m.removeFromQueueLocked(r)
	case StatusHandshaking:
		r.status = StatusCancelled
		m.releaseLocked(r)
		m.promoteLocked()
	case StatusEstablished:
		r.status = StatusClosed
		m.releaseLocked(r)
		h, closeIt = r.handle, true
		m.promoteLocked()
	default:
		// 已终结，无额外效果。
	}
	m.mu.Unlock()
	if closeIt {
		go m.broker.Close(h)
	}
}

// View 读取请求当前状态。
func (m *Manager) View(key string) Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.reqs[key]; ok {
		return r.status
	}
	return StatusUnknown
}

// Advance 推进从 0 开始、单调递增的逻辑毫秒时间，处理到期请求。
// 所有超时只依据此时间；截止时刻本身属于到期。
func (m *Manager) Advance(now int64) {
	m.mu.Lock()
	if now < m.now {
		m.mu.Unlock()
		return
	}
	m.now = now
	// 先处理全部到期，再统一推动队列；到期请求不得借同次推进释放的额度复活。
	for _, s := range m.sites {
		kept := s.queue[:0]
		for _, r := range s.queue {
			if now-r.queuedAt >= queueTimeout {
				r.status = StatusTimedOut
			} else {
				kept = append(kept, r)
			}
		}
		s.queue = kept
	}
	for _, r := range m.reqs {
		if r.status == StatusHandshaking && now >= r.deadline {
			r.status = StatusTimedOut
			m.releaseLocked(r)
		}
	}
	m.promoteLocked()
	m.mu.Unlock()
}

// removeFromQueueLocked 将排队请求从其站点队列中移除。
func (m *Manager) removeFromQueueLocked(r *request) {
	s := m.sites[r.site]
	for i, q := range s.queue {
		if q == r {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return
		}
	}
}

// releaseLocked 归还本请求持有的用户与站点额度。
// 只在从 Handshaking 或 Established 单向迁移到终结态时调用，保证只归还一次。
func (m *Manager) releaseLocked(r *request) {
	m.users[r.user]--
	m.sites[r.site].used--
}

// promoteLocked 按 FIFO 为各站点队首分配额度并发起握手。
// 队首因用户额度不足时该站点不再继续（不允许绕过）；
// 循环至不动点，使一次额度释放推动所有受影响站点。
func (m *Manager) promoteLocked() {
	for {
		progress := false
		for _, s := range m.sites {
			for len(s.queue) > 0 && s.used < s.capacity {
				head := s.queue[0]
				if m.users[head.user] >= userLimit {
					break
				}
				s.queue = s.queue[1:]
				s.used++
				m.users[head.user]++
				head.status = StatusHandshaking
				head.deadline = m.now + handshakeTimeout
				progress = true
				go m.handshake(head)
			}
		}
		if !progress {
			return
		}
	}
}

// handshake 在不持锁的情况下调用 Broker.Open；可能长时间阻塞。
func (m *Manager) handshake(r *request) {
	h, err := m.broker.Open(r.key)
	m.mu.Lock()
	if r.status != StatusHandshaking {
		// 请求已终结（取消或超时）：迟到成功必须关闭句柄，失败无需处理。
		m.mu.Unlock()
		if err == nil {
			go m.broker.Close(h)
		}
		return
	}
	if err != nil {
		r.status = StatusFailed
		m.releaseLocked(r)
		m.promoteLocked()
		m.mu.Unlock()
		return
	}
	r.status = StatusEstablished
	r.handle = h
	m.mu.Unlock()
}
