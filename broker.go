package rdsession

// Handle 是 Broker 返回的会话句柄，组件不透明地持有它。
type Handle = any

// Broker 由调用方实现，承担进程内握手。
// Open 可能长时间阻塞，最终返回唯一会话句柄或失败。
// Close 关闭句柄，保证成功，也可能长时间阻塞。
type Broker interface {
	Open(key string) (Handle, error)
	Close(handle Handle)
}
