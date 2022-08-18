// Copyright 2021 CloudWeGo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !race
// +build !race

package netpoll

import (
	"fmt"
	"log"
	"runtime"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/bytedance/gopkg/util/gopool"
)

// Includes defaultPoll/multiPoll/uringPoll...
func openPoll() Poll {
	return openDefaultPoll()
}

func openDefaultPoll() *defaultPoll {
	var poll = defaultPoll{}
	poll.buf = make([]byte, 8)
	var p, err = syscall.EpollCreate1(0)
	if err != nil {
		panic(err)
	}
	poll.fd = p
	var r0, _, e0 = syscall.Syscall(syscall.SYS_EVENTFD2, 0, 0, 0)
	if e0 != 0 {
		syscall.Close(p)
		panic(err)
	}

	poll.wop = &FDOperator{FD: int(r0)}
	poll.Control(poll.wop, PollReadable)
	return &poll
}

type defaultPoll struct {
	pollArgs
	fd      int         // epoll fd
	wop     *FDOperator // eventfd, wake epoll_wait
	buf     []byte      // read wfd trigger msg
	trigger uint32      // trigger flag

	hsnum int
	hs    []phandler
}

type pollArgs struct {
	size   int
	caps   int
	events []epollevent
}

func (a *pollArgs) reset(size, caps int) {
	a.size, a.events = size, make([]epollevent, size)
}

// Wait implements Poll.
func (p *defaultPoll) Wait() (err error) {
	// init
	var caps, msec, n = barriercap, -1, 0
	p.reset(128, caps)

	p.hsnum = runtime.GOMAXPROCS(0)
	p.hs = make([]phandler, p.hsnum)
	for i := 1; i < p.hsnum; i++ {
		p.hs[i].init(p.fd, p.wop.FD, p)
	}
	p.hs[0].pollfd, p.hs[0].wopfd, p.hs[0].size = p.fd, p.wop.FD, 128
	p.hs[0].gp = gopool.NewPool(fmt.Sprintf("%d", p.hs[0].pollfd), 10000, gopool.NewConfig())

	// wait
	for {
		if n == p.size && p.size < 128*1024 {
			p.reset(p.size<<1, caps)
		}
		n, err = EpollWait(p.fd, p.events, msec)
		if err != nil && err != syscall.EINTR {
			return err
		}
		if n <= 0 {
			msec = -1
			runtime.Gosched()
			continue
		}
		msec = 0
		if n >= p.hsnum {
			block := n / p.hsnum
			var idx, a, b int
			for idx = 1; idx < p.hsnum; idx++ {
				a = b
				b = idx * block
				p.hs[idx].work1 <- p.events[a:b]
			}
			p.hs[0].handler(p, p.events[b:n])
			for idx = 1; idx < p.hsnum; idx++ {
				<-p.hs[idx].work2
			}
		} else {
			p.hs[0].handler(p, p.events[:n])
		}
	}
}

type phandler struct {
	buf           [8]byte
	pollfd, wopfd int

	size    int
	trigger uint32 // trigger flag

	barriers []barrier
	hups     []func(p Poll) error

	work1 chan []epollevent
	work2 chan struct{}
	gp    gopool.Pool
}

func (p *phandler) init(pollfd, wopfd int, poll Poll) {
	p.pollfd, p.wopfd, p.size = pollfd, wopfd, 128
	p.work1 = make(chan []epollevent)
	p.work2 = make(chan struct{})
	p.gp = gopool.NewPool(fmt.Sprintf("%d", p.pollfd), 10000, gopool.NewConfig())
	go func() {
		for {
			events, ok := <-p.work1
			if !ok {
				return
			}
			p.handler(poll, events)
			p.work2 <- struct{}{}
		}
	}()
}

func (p *phandler) handler(poll Poll, events []epollevent) (closed bool) {
	// reset
	if n := len(events); n > len(p.barriers) {
		p.size <<= 1
		p.barriers = make([]barrier, p.size)
		for i := range p.barriers {
			p.barriers[i].bs = make([][]byte, barriercap)
			p.barriers[i].ivs = make([]syscall.Iovec, barriercap)
		}
	}

	for i := range events {
		var operator = *(**FDOperator)(unsafe.Pointer(&events[i].data))
		if !operator.do() {
			continue
		}
		// trigger or exit gracefully
		if operator.FD == p.wopfd {
			// must clean trigger first
			syscall.Read(p.wopfd, p.buf[:])
			atomic.StoreUint32(&p.trigger, 0)
			// if closed & exit
			if p.buf[0] > 0 {
				operator.done()
				return true
			}
			operator.done()
			continue
		}

		evt := events[i].events
		// check poll in
		if evt&syscall.EPOLLIN != 0 {
			if operator.OnRead != nil {
				// for non-connection
				operator.OnRead(poll)
			} else {
				// for connection
				var bs = operator.Inputs(p.barriers[i].bs)
				if len(bs) > 0 {
					var n, err = readv(operator.FD, bs, p.barriers[i].ivs)
					operator.runTask = p.gp.CtxGo
					operator.InputAck(n)
					if err != nil && err != syscall.EAGAIN && err != syscall.EINTR {
						log.Printf("readv(fd=%d) failed: %s", operator.FD, err.Error())
						p.appendHup(operator)
						continue
					}
				}
			}
		}

		// check hup
		if evt&(syscall.EPOLLHUP|syscall.EPOLLRDHUP) != 0 {
			p.appendHup(operator)
			continue
		}
		if evt&syscall.EPOLLERR != 0 {
			// Under block-zerocopy, the kernel may give an error callback, which is not a real error, just an EAGAIN.
			// So here we need to check this error, if it is EAGAIN then do nothing, otherwise still mark as hup.
			if _, _, _, _, err := syscall.Recvmsg(operator.FD, nil, nil, syscall.MSG_ERRQUEUE); err != syscall.EAGAIN {
				p.appendHup(operator)
			} else {
				operator.done()
			}
			continue
		}
		// check poll out
		if evt&syscall.EPOLLOUT != 0 {
			if operator.OnWrite != nil {
				// for non-connection
				operator.OnWrite(poll)
			} else {
				// for connection
				var bs, supportZeroCopy = operator.Outputs(p.barriers[i].bs)
				if len(bs) > 0 {
					// TODO: Let the upper layer pass in whether to use ZeroCopy.
					var n, err = sendmsg(operator.FD, bs, p.barriers[i].ivs, false && supportZeroCopy)
					operator.OutputAck(n)
					if err != nil && err != syscall.EAGAIN {
						log.Printf("sendmsg(fd=%d) failed: %s", operator.FD, err.Error())
						p.appendHup(operator)
						continue
					}
				}
			}
		}
		operator.done()
	}
	// hup conns together to avoid blocking the poll.
	p.detaches(poll)
	return false
}

// Close will write 10000000
func (p *defaultPoll) Close() error {
	_, err := syscall.Write(p.wop.FD, []byte{1, 0, 0, 0, 0, 0, 0, 0})
	return err
}

// Trigger implements Poll.
func (p *defaultPoll) Trigger() error {
	if atomic.AddUint32(&p.trigger, 1) > 1 {
		return nil
	}
	// MAX(eventfd) = 0xfffffffffffffffe
	_, err := syscall.Write(p.wop.FD, []byte{0, 0, 0, 0, 0, 0, 0, 1})
	return err
}

// Control implements Poll.
func (p *defaultPoll) Control(operator *FDOperator, event PollEvent) error {
	var op int
	var evt epollevent
	*(**FDOperator)(unsafe.Pointer(&evt.data)) = operator
	switch event {
	case PollReadable:
		operator.inuse()
		op, evt.events = syscall.EPOLL_CTL_ADD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollModReadable:
		operator.inuse()
		op, evt.events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollDetach:
		op, evt.events = syscall.EPOLL_CTL_DEL, syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollWritable:
		operator.inuse()
		op, evt.events = syscall.EPOLL_CTL_ADD, EPOLLET|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollR2RW:
		op, evt.events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollRW2R:
		op, evt.events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	}
	return EpollCtl(p.fd, op, operator.FD, &evt)
}

func (p *phandler) appendHup(operator *FDOperator) {
	p.hups = append(p.hups, operator.OnHup)
	operator.Control(PollDetach)
	operator.done()
}

func (p *phandler) detaches(poll Poll) {
	if len(p.hups) == 0 {
		return
	}
	hups := p.hups
	p.hups = nil
	go func(onhups []func(p Poll) error) {
		for i := range onhups {
			if onhups[i] != nil {
				onhups[i](poll)
			}
		}
	}(hups)
}
