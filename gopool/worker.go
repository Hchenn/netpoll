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
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/gopkg/util/logger"
)

var workerPool sync.Pool

func init() {
	workerPool.New = newWorker
}

type worker struct {
	ch   chan func()
	pool *pool
}

func newWorker() interface{} {
	w := &worker{}
	w.ch = make(chan func(), 1)
	return w
}

var newtask, reusetask, emptytask int32

func init() {
	go func() {
		for range time.Tick(time.Second) {
			n, r, e := atomic.LoadInt32(&newtask), atomic.LoadInt32(&reusetask), atomic.LoadInt32(&emptytask)
			fmt.Printf("new_task=%d, reuse_task=%d, empty_task=%d\n", n, r, e)
		}
	}()
}

func (w *worker) run(fn func()) {
	go func() {
		atomic.AddInt32(&newtask, 1)
		defer func() {
			if r := recover(); r != nil {
				ctx := context.Background()
				msg := fmt.Sprintf("GOPOOL: panic in pool: %s: %v: %s", w.pool.name, r, debug.Stack())
				logger.CtxErrorf(ctx, msg)
				if w.pool.panicHandler != nil {
					w.pool.panicHandler(ctx, r)
				}
			}
		}()
		fn()

		for w.pool.park(w.ch) {
			f, active := <-w.ch
			if !active {
				break
			}
			atomic.AddInt32(&reusetask, 1)
			f()
		}
		// if there's no task to do, exit
		w.close()
		w.Recycle()
		return
	}()
}

func (w *worker) close() {
	//w.pool.decWorkerCount()
}

func (w *worker) zero() {
	w.pool = nil
}

func (w *worker) Recycle() {
	w.zero()
	workerPool.Put(w)
}
