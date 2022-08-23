/*
 * Copyright 2021 CloudWeGo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gopool2

import (
	"context"
	"sync"
	"sync/atomic"
)

func NewPool() *Pool {
	p := &Pool{}
	p.assign = make([]chan func(), 0, 1024)
	p.take = make([]chan func(), 0, 1024)
	return p
}

type Pool struct {
	assign, take []chan func()
	lock         sync.Mutex
	idles        int32
}

func (p *Pool) CtxGo(ctx context.Context, f func()) {
	// 1. check idles
	if len(p.assign) == 0 && atomic.LoadInt32(&p.idles) > 0 {
		p.swap()
	}
	// reuse
	if l := len(p.assign); l > 0 {
		idx := l - 1
		assign := p.assign[idx]
		p.assign[idx] = nil
		p.assign = p.assign[:idx]
		assign <- f
		return
	}
	// new
	go func() {
		f()
		var ch = make(chan func())
		for {
			// append worker
			p.append(ch)
			// select
			t, ok := <-ch
			if !ok || t == nil {
				//close(ch)
				return
			}
			t()
		}
	}()
}

func (p *Pool) swap() {
	p.lock.Lock()
	tmp := p.assign
	p.assign = p.take
	p.take = tmp[:0]
	p.lock.Unlock()
	atomic.AddInt32(&p.idles, -int32(len(p.assign)))
}

func (p *Pool) append(worker chan func()) {
	p.lock.Lock()
	p.take = append(p.take, worker)
	p.lock.Unlock()
	atomic.AddInt32(&p.idles, 1)
}
