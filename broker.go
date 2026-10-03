package rdpadmit

// Handle 是 Broker 返回的会话句柄，组件不透明地持有并回传给 Close。
type Handle any

// Broker 由调用方实现，承担进程内握手。
// Open 最终返回唯一会话句柄或失败；Close 关闭句柄且保证成功。
// 两者都可能长时间阻塞，组件保证不在持锁状态下调用它们。
type Broker interface {
	Open(key string) (Handle, error)
	Close(handle Handle)
}
