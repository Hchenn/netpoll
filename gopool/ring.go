package gopool

// ring implements a fixed size ring buffer to manage data
type ring struct {
	arr  []chan<- func()
	size int
	tail int
	head int
	has  int
}

// newRing creates a ringbuffer with fixed size.
func newRing(size int) *ring {
	if size <= 0 {
		// When size is an invalid number, we still return an instance
		// with zero-size to reduce error checks of the callers.
		size = 0
	}
	r := &ring{
		arr:  make([]chan<- func(), size+1),
		size: size,
	}
	return r
}

// Push appends item to the ring.
func (r *ring) Push(i chan<- func()) error {
	r.arr[r.head] = i
	r.head = r.inc()
	return nil
}

// Pop returns the last item and removes it from the ring.
func (r *ring) Pop() chan<- func() {
	c := r.arr[r.tail]
	r.arr[r.tail] = nil
	r.tail = r.dec()
	return c
}

func (r *ring) inc() int {
	return (r.head + 1) % (r.size + 1)
}

func (r *ring) dec() int {
	return (r.tail + 1) % (r.size + 1)
}

func (r *ring) IsEmpty() bool {
	return r.tail == r.head
}

func (r *ring) IsFull() bool {
	return r.inc() == r.tail
}
