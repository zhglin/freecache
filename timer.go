package freecache

import (
	"sync/atomic"
	"time"
)

// Timer holds representation of current time.
// Timer表示当前时间。
type Timer interface {
	// Give current time (in seconds)
	// 获取当前的毫秒时间戳
	Now() uint32
}

// Timer that must be stopped.
// 必须停止的计时器。
type StoppableTimer interface {
	Timer

	// Release resources of the timer, functionality may or may not be affected
	// It is not called automatically, so user must call it just once
	// 释放资源的定时器，功能可能或可能不受影响。它不是自动调用，所以用户必须调用它只一次
	Stop()
}

// Helper function that returns Unix time in seconds
// 返回Unix时间(以秒为单位)的助手函数
func getUnixTime() uint32 {
	return uint32(time.Now().Unix())
}

// Default timer reads Unix time always when requested
// 默认定时器总是在被请求时读取Unix时间
type defaultTimer struct{}

func (timer defaultTimer) Now() uint32 {
	return getUnixTime()
}

// Cached timer stores Unix time every second and returns the cached value
// Cached定时器每秒钟存储Unix时间并返回缓存的值
type cachedTimer struct {
	now    uint32       // 当前的时间戳
	ticker *time.Ticker // 定时器
	done   chan bool
}

// Create cached timer and start runtime timer that updates time every second
// 创建缓存定时器和启动运行时定时器，每秒钟更新一次时间
func NewCachedTimer() StoppableTimer {
	timer := &cachedTimer{
		now:    getUnixTime(),
		ticker: time.NewTicker(time.Second), // 每秒钟定时器
		done:   make(chan bool),
	}

	// 更新now 当前时间
	go timer.update()

	return timer
}

// 返回当前时间
func (timer *cachedTimer) Now() uint32 {
	return atomic.LoadUint32(&timer.now)
}

// Stop runtime timer and finish routine that updates time
// 停止运行时定时器并完成更新时间的程序
func (timer *cachedTimer) Stop() {
	timer.ticker.Stop() // 停止计时器
	timer.done <- true  // 发送停止信号
	close(timer.done)

	timer.done = nil
	timer.ticker = nil
}

// Periodically check and update  of time
// 定期检查和更新时间
func (timer *cachedTimer) update() {
	for {
		select {
		case <-timer.done: // 协程退出
			return
		case <-timer.ticker.C:
			atomic.StoreUint32(&timer.now, getUnixTime())
		}
	}
}
