package bytebufferpool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoundedPoolZeroValue(t *testing.T) {
	var p BoundedPool

	b := p.Get()
	if b == nil {
		t.Fatalf("zero-value pool returned nil buffer")
	}
	if len(b.B) != 0 || cap(b.B) != minSize {
		t.Fatalf("unexpected buffer len/cap: %d/%d", len(b.B), cap(b.B))
	}
	b.WriteString("hello")
	if status := p.Put(b); status != ReleaseRetained {
		t.Fatalf("unexpected release status: %s", status)
	}

	stats := p.Stats()
	if stats.Allocations != 1 || stats.IdleBuffers != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	p.Close()
	p.Close() // idempotent
	if b := p.Get(); b != nil {
		t.Fatalf("Get returned a buffer after Close")
	}
}

func TestBoundedPoolReuseIsZeroed(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 4, MaxBytes: 1 << 20})

	b := p.Get()
	b.B = append(b.B, 's', 'e', 'c', 'r', 'e', 't')
	fullCap := b.B[:cap(b.B)]
	for i := range fullCap {
		fullCap[i] = 0xAB
	}
	b.B = b.B[:6]
	if status := p.Put(b); status != ReleaseRetained {
		t.Fatalf("unexpected release status: %s", status)
	}

	got := p.Get()
	if got != b {
		t.Fatalf("expected the retained buffer to be reused")
	}
	if len(got.B) != 0 {
		t.Fatalf("unexpected buffer length after reuse: %d", len(got.B))
	}
	for i, c := range got.B[:cap(got.B)] {
		if c != 0 {
			t.Fatalf("byte %d of reused buffer is not zeroed: %d", i, c)
		}
	}
}

func TestBoundedPoolPutRejectsUnknownBuffers(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 2, MaxBytes: 1 << 20})

	if status := p.Put(nil); status != ReleaseRejected {
		t.Fatalf("nil Put status = %s, want ReleaseRejected", status)
	}

	foreign := &ByteBuffer{}
	if status := p.Put(foreign); status != ReleaseRejected {
		t.Fatalf("foreign Put status = %s, want ReleaseRejected", status)
	}

	b := p.Get()
	if status := p.Put(b); status != ReleaseRetained {
		t.Fatalf("first Put status = %s, want ReleaseRetained", status)
	}
	if status := p.Put(b); status != ReleaseRejected {
		t.Fatalf("duplicate Put status = %s, want ReleaseRejected", status)
	}

	stats := p.Stats()
	if stats.RejectedPuts != 3 {
		t.Fatalf("RejectedPuts = %d, want 3", stats.RejectedPuts)
	}
	if stats.IdleBuffers != 1 {
		t.Fatalf("rejected Put changed pool inventory: %+v", stats)
	}

	// A buffer leased from another bounded pool is foreign to this one.
	other := NewBoundedPool(BoundedPoolConfig{})
	ob := other.Get()
	if status := p.Put(ob); status != ReleaseRejected {
		t.Fatalf("cross-pool Put status = %s, want ReleaseRejected", status)
	}
	other.Put(ob)
	other.Close()
}

func TestBoundedPoolPerClassLimit(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 2, MaxBytes: 1 << 20})

	// Lease three buffers, then return them: only two may be retained.
	held := make([]*ByteBuffer, 0, 3)
	for i := 0; i < 3; i++ {
		held = append(held, p.Get())
	}
	var retained, dropped int
	for _, b := range held {
		switch p.Put(b) {
		case ReleaseRetained:
			retained++
		case ReleaseDropped:
			dropped++
		default:
			t.Fatalf("unexpected release status")
		}
	}
	if retained != 2 || dropped != 1 {
		t.Fatalf("retained/dropped = %d/%d, want 2/1", retained, dropped)
	}

	stats := p.Stats()
	if stats.IdleBuffers != 2 {
		t.Fatalf("IdleBuffers = %d, want 2", stats.IdleBuffers)
	}
	if stats.OverLimitDrops != 1 {
		t.Fatalf("OverLimitDrops = %d, want 1", stats.OverLimitDrops)
	}

	p.mu.Lock()
	n := len(p.idle[index(minSize)])
	p.mu.Unlock()
	if n != 2 {
		t.Fatalf("per-class limit not applied: %d idle", n)
	}
}

func TestBoundedPoolByteBudget(t *testing.T) {
	// Room for exactly one buffer of the smallest class.
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 100, MaxBytes: minSize})

	held := make([]*ByteBuffer, 0, 2)
	for i := 0; i < 2; i++ {
		held = append(held, p.Get())
	}
	if status := p.Put(held[0]); status != ReleaseRetained {
		t.Fatalf("Put status = %s, want ReleaseRetained", status)
	}
	if status := p.Put(held[1]); status != ReleaseDropped {
		t.Fatalf("Put status = %s, want ReleaseDropped", status)
	}

	stats := p.Stats()
	if stats.OverLimitDrops != 1 {
		t.Fatalf("OverLimitDrops = %d, want 1", stats.OverLimitDrops)
	}
	if stats.RetainedBytes > minSize {
		t.Fatalf("RetainedBytes = %d exceeds budget %d", stats.RetainedBytes, minSize)
	}
}

func TestBoundedPoolOversizeDropped(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 4, MaxBytes: 1 << 30})

	b := p.GetSize(maxSize + 1)
	if cap(b.B) < maxSize+1 {
		t.Fatalf("GetSize returned capacity %d, want at least %d", cap(b.B), maxSize+1)
	}
	if status := p.Put(b); status != ReleaseDropped {
		t.Fatalf("Put status = %s, want ReleaseDropped", status)
	}
	stats := p.Stats()
	if stats.OversizeDrops != 1 {
		t.Fatalf("OversizeDrops = %d, want 1", stats.OversizeDrops)
	}
	if stats.RetainedBytes != 0 {
		t.Fatalf("oversize buffer was retained: %+v", stats)
	}
}

func TestBoundedPoolUndersizeDropped(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 4, MaxBytes: 1 << 20})

	b := p.Get()
	// The caller may replace the slice; a capacity that belongs to no class
	// must be dropped rather than stored somewhere else.
	b.B = make([]byte, 0, minSize/2)
	if status := p.Put(b); status != ReleaseDropped {
		t.Fatalf("Put status = %s, want ReleaseDropped", status)
	}
	stats := p.Stats()
	if stats.UndersizeDrops != 1 {
		t.Fatalf("UndersizeDrops = %d, want 1", stats.UndersizeDrops)
	}
	if stats.IdleBuffers != 0 || stats.RetainedBytes != 0 {
		t.Fatalf("undersize buffer was retained: %+v", stats)
	}
}

func TestBoundedPoolGetSizeIsolatesClasses(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 4, MaxBytes: 1 << 20})

	// A large response grows a buffer into a large class.
	big := p.GetSize(32 << 10)
	if cap(big.B) != 32<<10 {
		t.Fatalf("unexpected large capacity: %d", cap(big.B))
	}
	if status := p.Put(big); status != ReleaseRetained {
		t.Fatalf("Put status = %s", status)
	}

	// Small requests must not receive the large slice.
	small := p.Get()
	if cap(small.B) != minSize {
		t.Fatalf("Get returned a %d-byte buffer, want %d", cap(small.B), minSize)
	}
	p.Put(small)

	// The large class is reused for a request of that size.
	again := p.GetSize(32 << 10)
	if again != big {
		t.Fatalf("large buffer was not reused")
	}
	if cap(again.B) != 32<<10 {
		t.Fatalf("large buffer class changed: %d", cap(again.B))
	}
	if status := p.Put(again); status != ReleaseRetained {
		t.Fatalf("Put status = %s", status)
	}

	stats := p.Stats()
	if stats.Hits != 1 || stats.Allocations != 2 {
		t.Fatalf("unexpected hits/allocations: %+v", stats)
	}
	if stats.RetainedBytes != int64(minSize+32<<10) {
		t.Fatalf("unexpected retained bytes: %+v", stats)
	}
	checkBoundedInvariants(t, p, 4, 1<<20)
}

func TestBoundedPoolGetSizeSkipsTooSmallIdleBuffer(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 4, MaxBytes: 1 << 20})

	// A caller may return a slice with any capacity, so a class can hold
	// buffers of different capacities: 100 and 110 both belong to the
	// 65..128 class.
	first := p.Get()
	first.B = make([]byte, 0, 100)
	p.Put(first)

	// The most recent buffer is large enough for a request of 50 bytes.
	second := p.GetSize(110)
	if cap(second.B) < 110 {
		t.Fatalf("unexpected capacity: %d", cap(second.B))
	}
	p.Put(second)

	// A request that the smaller buffer cannot satisfy must not receive it,
	// even though it sits in the same class.
	got := p.GetSize(110)
	if cap(got.B) < 110 {
		t.Fatalf("GetSize(110) returned capacity %d", cap(got.B))
	}
	if got == first {
		t.Fatalf("GetSize(110) returned the 100-byte-capacity buffer")
	}

	// The smaller buffer is still available for a request that fits it.
	small := p.GetSize(100)
	if small != first {
		t.Fatalf("GetSize(100) did not reuse the smaller buffer")
	}
	p.Put(small)
	if status := p.Put(got); status != ReleaseRetained {
		t.Fatalf("Put status = %s", status)
	}

	checkBoundedInvariants(t, p, 4, 1<<20)
}

func TestBoundedPoolStaticHelpers(t *testing.T) {
	if got := classSize(0); got != minSize {
		t.Fatalf("classSize(0) = %d, want %d", got, minSize)
	}
	if got := classSize(steps - 1); got != maxSize {
		t.Fatalf("classSize(steps-1) = %d, want %d", got, maxSize)
	}
	for i := 0; i < steps; i++ {
		if idx := index(classSize(i)); idx != i {
			t.Fatalf("index(classSize(%d)) = %d", i, idx)
		}
	}

	statuses := map[ReleaseStatus]string{
		ReleaseRetained: "retained",
		ReleaseDropped:  "dropped",
		ReleaseRejected: "rejected",
		ReleaseReleased: "released",
	}
	for status, want := range statuses {
		if got := status.String(); got != want {
			t.Fatalf("ReleaseStatus(%d).String() = %q, want %q", status, got, want)
		}
	}
	if got := ReleaseStatus(255).String(); got == "" {
		t.Fatalf("unknown release status rendered empty")
	}
}

func TestBoundedPoolStatsPartition(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 1, MaxBytes: 1 << 20})

	// One miss followed by one hit.
	b := p.Get()
	if status := p.Put(b); status != ReleaseRetained {
		t.Fatalf("Put status = %s", status)
	}
	b = p.Get()
	if status := p.Put(b); status != ReleaseRetained {
		t.Fatalf("Put status = %s", status)
	}

	stats := p.Stats()
	if stats.Allocations != 1 {
		t.Fatalf("Allocations = %d, want 1", stats.Allocations)
	}
	if stats.Hits != 1 {
		t.Fatalf("Hits = %d, want 1", stats.Hits)
	}
	if stats.RetainedPuts != 2 {
		t.Fatalf("RetainedPuts = %d, want 2", stats.RetainedPuts)
	}
	if stats.IdleBuffers != 1 || stats.RetainedBytes != int64(minSize) {
		t.Fatalf("unexpected inventory: %+v", stats)
	}
	if stats.LeasedBuffers != 0 {
		t.Fatalf("LeasedBuffers = %d, want 0", stats.LeasedBuffers)
	}
	if stats.DefaultCapacity != minSize {
		t.Fatalf("DefaultCapacity = %d, want %d", stats.DefaultCapacity, minSize)
	}
}

func TestBoundedPoolCloseKeepsLeasesUsable(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 4, MaxBytes: 1 << 20})

	leased := p.Get()
	leased.WriteString("still usable")

	idle := p.Get()
	if status := p.Put(idle); status != ReleaseRetained {
		t.Fatalf("Put status = %s", status)
	}

	p.Close()

	if string(leased.B) != "still usable" {
		t.Fatalf("lease was modified or reused by Close: %q", leased.B)
	}
	if b := p.Get(); b != nil {
		t.Fatalf("Get returned a buffer after Close")
	}
	if b := p.GetSize(1024); b != nil {
		t.Fatalf("GetSize returned a buffer after Close")
	}
	if !p.Closed() {
		t.Fatalf("Closed reported false after Close")
	}

	if status := p.Put(leased); status != ReleaseReleased {
		t.Fatalf("Put after Close status = %s, want ReleaseReleased", status)
	}
	// The released lease must not be retained or reused.
	if string(leased.B) != "still usable" {
		t.Fatalf("released lease was zeroed or reset by Put: %q", leased.B)
	}

	stats := p.Stats()
	if stats.IdleBuffers != 0 || stats.RetainedBytes != 0 {
		t.Fatalf("Close retained storage: %+v", stats)
	}
	if stats.ClosedReleases != 1 {
		t.Fatalf("ClosedReleases = %d, want 1", stats.ClosedReleases)
	}
	if stats.ClosedGets != 2 {
		t.Fatalf("ClosedGets = %d, want 2", stats.ClosedGets)
	}
}

func TestBoundedPoolCloseWakesWaiters(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxLeasedBuffers: 1, MaxBytes: 1 << 20})

	held := p.Get()

	var got atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		got.Store(p.Get() != nil)
	}()

	// Give the waiter a chance to block, then close.
	deadline := time.Now().Add(2 * time.Second)
	for p.Stats().BlockedGets == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	p.Close()
	p.Close()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("Close did not wake the blocked Get")
	}

	if v, _ := got.Load().(bool); v {
		t.Fatalf("Get returned a buffer after Close")
	}
	if status := p.Put(held); status != ReleaseReleased {
		t.Fatalf("Put after Close status = %s", status)
	}
}

func TestBoundedPoolLeaseLimitSerializes(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxLeasedBuffers: 2, MaxBuffersPerClass: 8, MaxBytes: 1 << 20})

	a := p.Get()
	b := p.Get()

	acquired := make(chan struct{})
	go func() {
		c := p.Get()
		if c != nil {
			p.Put(c)
		}
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatalf("Get did not block at the lease limit")
	case <-time.After(20 * time.Millisecond):
	}

	p.Put(a)
	select {
	case <-acquired:
	case <-time.After(3 * time.Second):
		t.Fatalf("returning a lease did not wake Get")
	}
	p.Put(b)
}

func TestBoundedPoolConcurrentGetPut(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 4, MaxBytes: 64 << 10})

	const goroutines = 8
	const iterations = 2000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				n := 1 << uint((seed+i)%8)
				b := p.GetSize(n)
				if b == nil {
					t.Errorf("unexpected nil buffer")
					return
				}
				if len(b.B) != 0 {
					t.Errorf("non-empty buffer returned: len %d", len(b.B))
					return
				}
				// A freshly leased buffer must be zeroed before use.
				for j, c := range b.B[:cap(b.B)] {
					if c != 0 {
						t.Errorf("byte %d is not zeroed: %d", j, c)
						break
					}
				}
				b.B = b.B[:cap(b.B)]
				status := p.Put(b)
				if status != ReleaseRetained && status != ReleaseDropped {
					t.Errorf("unexpected release status: %s", status)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	stats := p.Stats()
	if stats.LeasedBuffers != 0 {
		t.Fatalf("leases leaked: %+v", stats)
	}
	if stats.RetainedBytes > 64<<10 {
		t.Fatalf("byte budget exceeded: %d", stats.RetainedBytes)
	}
	if stats.Hits+stats.Allocations != uint64(goroutines*iterations) {
		t.Fatalf("hits+allocations = %d, want %d", stats.Hits+stats.Allocations, goroutines*iterations)
	}

	checkBoundedInvariants(t, p, 4, 64<<10)
}

// checkBoundedInvariants verifies that idle bookkeeping matches the idle
// buffers themselves while the pool is idle.
func checkBoundedInvariants(t *testing.T, p *BoundedPool, maxPerClass int, maxBytes int64) {
	t.Helper()

	p.mu.Lock()
	defer p.mu.Unlock()

	var total int64
	count := 0
	for i := range p.idle {
		if len(p.idle[i]) > maxPerClass {
			t.Fatalf("class %d exceeds per-class limit: %d", i, len(p.idle[i]))
		}
		for _, b := range p.idle[i] {
			if idx := index(cap(b.B)); idx != i {
				t.Fatalf("buffer with cap %d stored in class %d, want %d", cap(b.B), i, idx)
			}
			if len(b.B) != 0 {
				t.Fatalf("idle buffer is not reset: len %d", len(b.B))
			}
			total += int64(cap(b.B))
			count++
		}
	}
	if total > maxBytes {
		t.Fatalf("retained bytes %d exceed budget %d", total, maxBytes)
	}
	if total != p.retainedBytes {
		t.Fatalf("inventory mismatch: %d != %d", total, p.retainedBytes)
	}
	if count != p.idleBuffers {
		t.Fatalf("idle count mismatch: %d != %d", count, p.idleBuffers)
	}
}

func TestBoundedPoolCalibrationPicksHotClass(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 8, MaxBytes: 1 << 30})

	// Drive calibration with a single hot size, crossing the threshold.
	const hot = 4096
	for i := 0; i < calibrateCallsThreshold+1; i++ {
		b := p.GetSize(hot)
		b.B = b.B[:hot]
		p.Put(b)
	}

	stats := p.Stats()
	if stats.Calibrations == 0 {
		t.Fatalf("calibration never ran")
	}
	if stats.DefaultCapacity != hot {
		t.Fatalf("default capacity = %d, want %d", stats.DefaultCapacity, hot)
	}
	if b := p.Get(); cap(b.B) != hot {
		t.Fatalf("Get used capacity %d, want %d", cap(b.B), hot)
	} else {
		p.Put(b)
	}

	checkBoundedInvariants(t, p, 8, 1<<30)
}

func TestBoundedPoolConcurrentCalibrateAndPut(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 2, MaxBytes: 1 << 16})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Hammer GetSize/Put with mixed sizes while calibration runs concurrently.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				b := p.GetSize(1 << uint((seed+i)%12))
				if b == nil {
					return
				}
				b.B = b.B[:cap(b.B)]
				if status := p.Put(b); status != ReleaseRetained && status != ReleaseDropped {
					t.Errorf("unexpected status: %s", status)
					return
				}
			}
		}(g)
	}

	// Force many calibrations by calling Put beyond the threshold.
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		b := p.Get()
		p.Put(b)
		time.Sleep(time.Microsecond)
	}
	close(stop)
	wg.Wait()

	checkBoundedInvariants(t, p, 2, 1<<16)
}

func TestBoundedPoolConcurrentClose(t *testing.T) {
	p := NewBoundedPool(BoundedPoolConfig{MaxBuffersPerClass: 4, MaxBytes: 1 << 20})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				b := p.GetSize(1 << uint(seed%10))
				if b == nil {
					return
				}
				b.WriteString("x")
				// Every buffer must be retained, dropped or released, never
				// rejected: it was leased by this pool.
				status := p.Put(b)
				switch status {
				case ReleaseRetained, ReleaseDropped, ReleaseReleased:
				default:
					t.Errorf("unexpected status: %s", status)
					return
				}
			}
		}(i)
	}

	// Close repeatedly while workers run.
	for i := 0; i < 10; i++ {
		p.Close()
		time.Sleep(time.Millisecond)
	}
	wg.Wait()

	stats := p.Stats()
	if stats.IdleBuffers != 0 || stats.RetainedBytes != 0 {
		t.Fatalf("closed pool retains storage: %+v", stats)
	}
	if stats.RejectedPuts != 0 {
		t.Fatalf("valid leases were rejected: %+v", stats)
	}
	if stats.LeasedBuffers != 0 {
		t.Fatalf("leases leaked: %+v", stats)
	}
}
