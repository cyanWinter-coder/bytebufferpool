package bytebufferpool_test

import (
	"fmt"
	"sync"

	"github.com/valyala/bytebufferpool"
)

func ExampleBoundedPool() {
	pool := bytebufferpool.NewBoundedPool(bytebufferpool.BoundedPoolConfig{
		MaxBuffersPerClass: 4,
		MaxBytes:           1 << 20,
	})

	bb := pool.Get()
	bb.WriteString("response body")
	fmt.Printf("contents=%q\n", bb.B)

	// Returning the buffer zeroes it and keeps it for reuse, unless the
	// per-class limit or the byte budget is already reached.
	status := pool.Put(bb)
	fmt.Printf("release=%s\n", status)

	fmt.Printf("idle=%d retained=%d\n", pool.Stats().IdleBuffers, pool.Stats().RetainedBytes)

	// Close releases everything that is idle. Buffers leased before Close stay
	// usable; returning them then releases them for good.
	pool.Close()
	// Output:
	// contents="response body"
	// release=retained
	// idle=1 retained=64
}

func ExampleBoundedPool_concurrent() {
	pool := bytebufferpool.NewBoundedPool(bytebufferpool.BoundedPoolConfig{
		MaxBuffersPerClass: 8,
		MaxBytes:           4 << 20,
	})

	var wg sync.WaitGroup
	ids := make(chan *bytebufferpool.ByteBuffer, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bb := pool.GetSize(4096)
			bb.WriteString("ok")
			ids <- bb
		}()
	}

	// Return the buffers only after all of them were leased, so that four
	// distinct buffers are retained rather than the same one reused.
	wg.Wait()
	close(ids)
	for bb := range ids {
		pool.Put(bb)
	}

	stats := pool.Stats()
	fmt.Printf("leased=%d idle=%d underBudget=%t\n",
		stats.LeasedBuffers, stats.IdleBuffers, stats.RetainedBytes <= 4<<20)
	// Output:
	// leased=0 idle=4 underBudget=true
}
