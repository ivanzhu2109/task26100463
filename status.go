package rdsession

// Status 表示请求的当前状态。
type Status int

const (
	// StatusUnknown 表示 key 不存在。
	StatusUnknown Status = iota
	// StatusQueued 排队中，不占用任何额度。
	StatusQueued
	// StatusHandshaking 已获得用户与站点额度，握手进行中。
	StatusHandshaking
	// StatusEstablished 握手成功，会话已建立。
	StatusEstablished
	// StatusCancelled 已取消（终结）。
	StatusCancelled
	// StatusTimedOut 已超时（终结）。
	StatusTimedOut
	// StatusFailed 握手失败（终结）。
	StatusFailed
	// StatusClosed 已建立的会话被 Cancel 结束（终结）。
	StatusClosed
)

// String 返回状态的可读名称。
func (s Status) String() string {
	switch s {
	case StatusQueued:
		return "Queued"
	case StatusHandshaking:
		return "Handshaking"
	case StatusEstablished:
		return "Established"
	case StatusCancelled:
		return "Cancelled"
	case StatusTimedOut:
		return "TimedOut"
	case StatusFailed:
		return "Failed"
	case StatusClosed:
		return "Closed"
	default:
		return "Unknown"
	}
}
