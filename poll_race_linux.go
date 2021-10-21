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

// +build race

package netpoll

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
)

func openDefaultPoll() *defaultPoll {
	var poll = &defaultPoll{}
	poll.InitEpoll(poll)
	return poll
}

type defaultPoll struct {
	baseEpoll
	pollArgs
}

type pollArgs struct {
	size     int
	caps     int
	events   []syscall.EpollEvent
	barriers []barrier
	m        sync.Map
}

func (a *pollArgs) reset(size, caps int) {
	a.size, a.caps = size, caps
	a.events, a.barriers = make([]syscall.EpollEvent, size), make([]barrier, size)
	for i := range a.barriers {
		a.barriers[i].bs = make([][]byte, a.caps)
		a.barriers[i].ivs = make([]syscall.Iovec, a.caps)
	}
}

// Control implements Poll.
func (p *defaultPoll) Control(operator *FDOperator, event PollEvent) error {
	var op int
	var evt syscall.EpollEvent
	evt.Fd = int32(operator.FD)
	switch event {
	case PollReadable:
		operator.inuse()
		p.m.Store(operator.FD, operator)
		op, evt.Events = syscall.EPOLL_CTL_ADD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollModReadable:
		operator.inuse()
		p.m.Store(operator.FD, operator)
		op, evt.Events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollDetach:
		defer operator.unused()
		p.m.Delete(operator.FD)
		op, evt.Events = syscall.EPOLL_CTL_DEL, syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollWritable:
		operator.inuse()
		p.m.Store(operator.FD, operator)
		op, evt.Events = syscall.EPOLL_CTL_ADD, EPOLLET|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollR2RW:
		op, evt.Events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollRW2R:
		op, evt.Events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	}
	return syscall.EpollCtl(p.fd, op, operator.FD, &evt)
}

// Wait implements Poll.
func (p *defaultPoll) Wait() (err error) {
	// init
	var caps, msec, n = barriercap, -1, 0
	p.reset(128, caps)
	// wait
	for {
		if n == p.size && p.size < 128*1024 {
			p.reset(p.size<<1, caps)
		}
		n, err = syscall.EpollWait(p.fd, p.events, msec)
		if err != nil && err != syscall.EINTR {
			return err
		}
		if n <= 0 {
			msec = -1
			runtime.Gosched()
			continue
		}
		msec = 0
		if p.handler(p.events[:n]) {
			return nil
		}
	}
}

func (p *defaultPoll) handler(events []syscall.EpollEvent) (closed bool) {
	var hups []*FDOperator
	for i := range events {
		var operator *FDOperator
		if tmp, ok := p.m.Load(int(events[i].Fd)); ok {
			operator = tmp.(*FDOperator)
		} else {
			continue
		}
		// trigger or exit gracefully
		if operator.FD == p.wop.FD {
			// must clean trigger first
			syscall.Read(p.wop.FD, p.buf)
			atomic.StoreUint32(&p.trigger, 0)
			// if closed & exit
			if p.buf[0] > 0 {
				syscall.Close(p.wop.FD)
				syscall.Close(p.fd)
				return true
			}
			continue
		}
		if !operator.do() {
			continue
		}

		evt := events[i].Events
		switch {
		// check hup first
		case evt&(syscall.EPOLLHUP|syscall.EPOLLRDHUP) != 0:
			hups = append(hups, operator)
		case evt&syscall.EPOLLERR != 0:
			// Under block-zerocopy, the kernel may give an error callback, which is not a real error, just an EAGAIN.
			// So here we need to check this error, if it is EAGAIN then do nothing, otherwise still mark as hup.
			if _, _, _, _, err := syscall.Recvmsg(operator.FD, nil, nil, syscall.MSG_ERRQUEUE); err != syscall.EAGAIN {
				hups = append(hups, operator)
			}
		default:
			if evt&syscall.EPOLLIN != 0 {
				if operator.OnRead != nil {
					// for non-connection
					operator.OnRead(p)
				} else {
					// for connection
					var bs = operator.Inputs(p.barriers[i].bs)
					if len(bs) > 0 {
						var n, err = readv(operator.FD, bs, p.barriers[i].ivs)
						operator.InputAck(n)
						if err != nil && err != syscall.EAGAIN && err != syscall.EINTR {
							log.Printf("readv(fd=%d) failed: %s", operator.FD, err.Error())
							hups = append(hups, operator)
							break
						}
					}
				}
			}
			if evt&syscall.EPOLLOUT != 0 {
				if operator.OnWrite != nil {
					// for non-connection
					operator.OnWrite(p)
				} else {
					// for connection
					var bs, supportZeroCopy = operator.Outputs(p.barriers[i].bs)
					if len(bs) > 0 {
						// TODO: Let the upper layer pass in whether to use ZeroCopy.
						var n, err = sendmsg(operator.FD, bs, p.barriers[i].ivs, false && supportZeroCopy)
						operator.OutputAck(n)
						if err != nil && err != syscall.EAGAIN {
							log.Printf("sendmsg(fd=%d) failed: %s", operator.FD, err.Error())
							hups = append(hups, operator)
							break
						}
					}
				}
			}
		}
		operator.done()
	}
	// hup conns together to avoid blocking the poll.
	if len(hups) > 0 {
		p.detaches(p, hups)
	}
	return false
}
