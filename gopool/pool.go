// Copyright 2021 ByteDance Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gopool

import (
	"context"
	"sync"
	"sync/atomic"
)

type Pool interface {
	// Name returns the corresponding pool name.
	Name() string
	// SetCap sets the goroutine capacity of the pool.
	SetCap(cap int32)
	// Go executes f.
	Go(f func())

	CtxGo(ctx context.Context, f func())
	// CtxGo executes f and accepts the context.
	CtxGos(ctx context.Context, f ...func())
	// SetPanicHandler sets the panic handler.
	SetPanicHandler(f func(context.Context, interface{}))
}

//
//var taskPool sync.Pool
//
//func init() {
//	taskPool.New = newTask
//}
//
//type task struct {
//	ctx context.Context
//	f   func()
//
//	next *task
//}
//
//func (t *task) zero() {
//	t.ctx = nil
//	t.f = nil
//	t.next = nil
//}
//
//func (t *task) Recycle() {
//	t.zero()
//	taskPool.Put(t)
//}
//
//func newTask() interface{} {
//	return &task{}
//}
//
//type taskList struct {
//	sync.Mutex
//	taskHead *task
//	taskTail *task
//}

type pool struct {
	lock     sync.Mutex
	idles    *ring
	idleSize int

	// The name of the pool
	name string

	// capacity of the pool, the maximum number of goroutines that are actually working
	cap int
	// Configuration information
	config *Config
	// linked list of tasks
	//taskHead *task
	//taskTail *task
	//taskLock sync.Mutex
	//taskCount int32

	// Record the number of running workers
	workerCount int32

	// This method will be called when the worker panic
	panicHandler func(context.Context, interface{})
}

// NewPool creates a new pool with the given name, cap and config.
func NewPool(name string, cap int32, config *Config) Pool {
	p := &pool{
		name: name,
		//cap:    cap,
		config: config,
	}
	p.idles = newRing(10000)
	return p
}

func (p *pool) Name() string {
	return p.name
}

func (p *pool) SetCap(cap int32) {
	//atomic.StoreInt32(&p.cap, cap)
}

func (p *pool) Go(f func()) {
	//p.CtxGo(context.Background(), f)
	go f()
}

func (p *pool) CtxGo(ctx context.Context, f func()) {
	p.lock.Lock()
	if p.idleSize > 0 {
		ch := p.idles.Pop()
		ch <- f
		p.idleSize -= 1
		p.lock.Unlock()
		return
	}
	p.lock.Unlock()
	w := workerPool.Get().(*worker)
	w.pool = p
	w.run(f)
}

func (p *pool) CtxGos(ctx context.Context, f ...func()) {
	var i int
	p.lock.Lock()
	for p.idleSize > 0 && len(f) > 0 {
		for i = 0; i < len(f) && i < p.idleSize; i++ {
			ch := p.idles.Pop()
			ch <- f[i]
		}
		f = f[i:]
		p.idleSize -= i
	}
	p.lock.Unlock()
	for i = 0; i < len(f); i++ {
		w := workerPool.Get().(*worker)
		w.pool = p
		w.run(f[i])
	}
}

func (p *pool) park(notify chan<- func()) (ok bool) {
	p.lock.Lock()
	if p.idles.IsFull() {
		p.lock.Unlock()
		return false
	}
	p.idles.Push(notify)
	p.idleSize += 1
	p.lock.Unlock()
	return true
}

// SetPanicHandler the func here will be called after the panic has been recovered.
func (p *pool) SetPanicHandler(f func(context.Context, interface{})) {
	p.panicHandler = f
}

func (p *pool) WorkerCount() int32 {
	return atomic.LoadInt32(&p.workerCount)
}

func (p *pool) incWorkerCount() {
	atomic.AddInt32(&p.workerCount, 1)
}

func (p *pool) decWorkerCount() {
	atomic.AddInt32(&p.workerCount, -1)
}
