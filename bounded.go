package bytebufferpool

import (
	"fmt"
	"sync"
)

const (
	// DefaultBoundedMaxBuffersPerClass is the default number of idle buffers
	// retained per capacity class by a BoundedPool.
	DefaultBoundedMaxBuffersPerClass = 8

	// DefaultBoundedMaxBytes is the default total capacity, in bytes, a
	// BoundedPool may retain across all capacity classes.
	DefaultBoundedMaxBytes = 32 << 20
)

// BoundedPoolConfig configures a BoundedPool.
//
// The zero value selects the default limits documented on each field.
type BoundedPoolConfig struct {
	// MaxBuffersPerClass is the fixed number of idle buffers kept per capacity
	// class. A returned buffer whose class is already full is released
	// immediately instead of retained.
	//
	// Zero selects DefaultBoundedMaxBuffersPerClass; a negative value removes
	// the per-class limit.
	MaxBuffersPerClass int

	// MaxBytes is the total capacity, in bytes, of all idle buffers retained
	// by the pool. Capacity rather than length is accounted, because capacity
	// is what keeps memory alive. A returned buffer that does not fit the
	// budget is released immediately instead of retained.
	//
	// Zero selects DefaultBoundedMaxBytes; a negative value removes the byte
	// budget.
	MaxBytes int64

	// MaxLeasedBuffers bounds the number of buffers handed out by Get and
	// GetSize and not yet returned via Put. When the limit is reached Get
	// blocks until a buffer is returned or the pool is closed. Zero or a
	// negative value removes the limit, so Get never blocks.
	//
	// This bounds buffers in active use; MaxBytes bounds idle storage.
	MaxLeasedBuffers int
}

// ReleaseStatus describes what a BoundedPool did with a buffer passed to Put.
type ReleaseStatus uint8

const (
	// ReleaseRetained means the buffer was an outstanding lease, was zeroed
	// and is now retained for reuse.
	ReleaseRetained ReleaseStatus = iota

	// ReleaseDropped means the buffer was an outstanding lease that could not
	// be retained: the per-class limit or the byte budget was reached, or its
	// capacity cannot be assigned a capacity class. The buffer becomes
	// garbage.
	ReleaseDropped

	// ReleaseRejected means the buffer was not an outstanding lease of this
	// pool: it was nil, a duplicate return, or a buffer obtained elsewhere.
	// Pool state is left untouched and the buffer is neither zeroed nor
	// retained.
	ReleaseRejected

	// ReleaseReleased means the buffer was an outstanding lease returned after
	// Close. It is not retained.
	ReleaseReleased
)

// String returns the name of s.
func (s ReleaseStatus) String() string {
	switch s {
	case ReleaseRetained:
		return "retained"
	case ReleaseDropped:
		return "dropped"
	case ReleaseRejected:
		return "rejected"
	case ReleaseReleased:
		return "released"
	default:
		return fmt.Sprintf("ReleaseStatus(%d)", uint8(s))
	}
}

// BoundedPool is a closable byte buffer pool with hard bounds on retained
// memory.
//
// Pool retains buffers according to their capacity and lets calibration pick
// the class used by Get, so after a burst of large responses it may keep a
// large amount of memory and small requests may share slices grown by large
// ones. BoundedPool keeps a fixed number of buffers per capacity class plus a
// total byte budget, releases anything that does not fit on Put, and serves
// GetSize only from the class matching the requested size.
//
// A buffer is always stored in the class derived from cap(b.B) while the pool
// lock is held, and is always removed from that same class. Calibration only
// selects the class used by future Get calls, so concurrent calibration and
// eviction cannot move a buffer into the wrong class. Buffers are zeroed over
// their whole capacity before being retained, so Get and GetSize only hand out
// zeroed, safely reusable buffers.
//
// Put accepts a buffer only when it is an outstanding lease of this pool.
// Nil, duplicate and foreign buffers are rejected before any pool state
// changes. A dropped buffer stops being a lease, so a second return of it is
// rejected as well.
//
// Buffers leased before Close stay usable: Put accepts them as ReleaseReleased
// and does not retain them. Close never reclaims or reuses a buffer that is
// still leased. Get and GetSize return nil after Close.
//
// A BoundedPool must not be copied after first use; share its pointer. Its
// zero value is ready to use with the default limits.
type BoundedPool struct {
	initOnce sync.Once

	mu     sync.Mutex
	cond   *sync.Cond
	closed bool

	cfg BoundedPoolConfig

	// idle holds idle buffers per capacity class. A buffer is appended using
	// index(cap(b.B)) and removed from the same class, so a buffer can never
	// be served from a class other than the one it was stored in.
	idle          [steps][]*ByteBuffer
	idleBuffers   int
	retainedBytes int64

	// leased holds buffers handed out by Get and GetSize and not yet returned.
	// Put accepts a buffer only if it is present here, which rejects duplicate
	// and foreign returns without touching pool state. Entries are kept across
	// Close so leases taken before Close can still be recognized.
	leased map[*ByteBuffer]struct{}

	// calls counts Put sizes since the last calibration; defaultClass is the
	// class used by Get.
	calls                [steps]uint64
	putsSinceCalibration uint64
	defaultClass         int

	stats boundedPoolStats
}

type boundedPoolStats struct {
	hits           uint64
	allocations    uint64
	retainedPuts   uint64
	overLimitDrops uint64
	oversizeDrops  uint64
	undersizeDrops uint64
	rejectedPuts   uint64
	closedReleases uint64
	closedGets     uint64
	blockedGets    uint64
	calibrations   uint64
	zeroedBytes    uint64
}

// BoundedPoolStats is a snapshot of the activity and inventory of a
// BoundedPool.
//
// Hits and Allocations partition the Get and GetSize calls that returned a
// buffer: each such call either reused an idle buffer or allocated a new one.
// OverLimitDrops, OversizeDrops and UndersizeDrops partition the Put calls
// whose outstanding lease was dropped instead of retained. RejectedPuts counts
// returns refused because they were not outstanding leases. All fields are
// sampled while the pool lock is held, so inventory and counters in the same
// snapshot are consistent with each other.
type BoundedPoolStats struct {
	// Hits is the number of Get or GetSize calls served from idle storage.
	Hits uint64

	// Allocations is the number of Get or GetSize calls that allocated a new
	// buffer.
	Allocations uint64

	// RetainedPuts is the number of Put calls that zeroed and retained a
	// buffer.
	RetainedPuts uint64

	// OverLimitDrops is the number of Put calls whose buffer was dropped
	// because the per-class limit or the byte budget was reached.
	OverLimitDrops uint64

	// OversizeDrops is the number of Put calls whose capacity exceeded the
	// largest capacity class.
	OversizeDrops uint64

	// UndersizeDrops is the number of Put calls whose capacity was below the
	// smallest capacity class and therefore could not be assigned to one.
	UndersizeDrops uint64

	// RejectedPuts is the number of Put calls rejected as nil, duplicate or
	// foreign returns.
	RejectedPuts uint64

	// ClosedReleases is the number of outstanding leases released after Close.
	ClosedReleases uint64

	// ClosedGets is the number of Get or GetSize calls made after Close.
	ClosedGets uint64

	// BlockedGets is the number of Get or GetSize calls that waited for a
	// lease slot because MaxLeasedBuffers was reached.
	BlockedGets uint64

	// Calibrations is the number of times the default class was recalibrated.
	Calibrations uint64

	// ZeroedBytes is the total capacity zeroed before retention.
	ZeroedBytes uint64

	// IdleBuffers is the number of idle buffers currently retained.
	IdleBuffers int

	// RetainedBytes is the total capacity of idle buffers, in bytes.
	RetainedBytes int64

	// LeasedBuffers is the number of buffers currently leased and not yet
	// returned.
	LeasedBuffers int

	// DefaultCapacity is the capacity class used by Get.
	DefaultCapacity int
}

// NewBoundedPool returns an open pool with the given configuration. The zero
// configuration selects the defaults documented in BoundedPoolConfig.
func NewBoundedPool(cfg BoundedPoolConfig) *BoundedPool {
	p := &BoundedPool{cfg: cfg}
	p.init()
	return p
}

func (p *BoundedPool) init() {
	p.initOnce.Do(func() {
		if p.cfg.MaxBuffersPerClass == 0 {
			p.cfg.MaxBuffersPerClass = DefaultBoundedMaxBuffersPerClass
		}
		if p.cfg.MaxBytes == 0 {
			p.cfg.MaxBytes = DefaultBoundedMaxBytes
		}
		if p.cfg.MaxLeasedBuffers < 0 {
			p.cfg.MaxLeasedBuffers = 0
		}
		p.cond = sync.NewCond(&p.mu)
		p.leased = make(map[*ByteBuffer]struct{})
	})
}

// Get returns an empty byte buffer from the pool.
//
// The returned buffer is a zeroed outstanding lease: it may be used freely and
// must be returned via Put. Its capacity is the calibrated default class, so a
// buffer grown by a large response is never served to a request of the default
// size.
//
// Get returns nil after Close. If MaxLeasedBuffers is configured and the limit
// is reached, Get blocks until another lease is returned or the pool is closed.
func (p *BoundedPool) Get() *ByteBuffer {
	p.init()

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.waitForLeaseLocked() {
		return nil
	}
	return p.leaseLocked(p.defaultClass, 0, classSize(p.defaultClass))
}

// GetSize returns an empty byte buffer with capacity of at least size.
//
// The buffer is taken from the smallest capacity class able to hold size,
// which isolates workloads of different sizes from one another. A size larger
// than the largest capacity class is served by an unpooled buffer of exactly
// that capacity; Put drops such a buffer instead of retaining it.
//
// Like Get, the returned buffer is a zeroed outstanding lease, GetSize returns
// nil after Close, and it blocks at the MaxLeasedBuffers limit.
func (p *BoundedPool) GetSize(size int) *ByteBuffer {
	p.init()

	if size < 0 {
		size = 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.waitForLeaseLocked() {
		return nil
	}
	if size > maxSize {
		return p.leaseLocked(-1, 0, size)
	}
	idx := index(size)
	return p.leaseLocked(idx, size, classSize(idx))
}

// leaseLocked leases an idle buffer of class idx with capacity of at least
// need, allocating one with allocSize capacity when the class holds no suitable
// buffer. A negative idx leases an unpooled buffer that Put drops. p.mu must be
// held.
func (p *BoundedPool) leaseLocked(idx, need, allocSize int) *ByteBuffer {
	if idx >= 0 {
		// Idle buffers of a class may have capacities that only fall into that
		// class, e.g. when a caller grew a slice with append. A buffer that is
		// too small for this request must not be handed out, so scan from the
		// most recently used buffer downwards and take the first that fits.
		bufs := p.idle[idx]
		for i := len(bufs) - 1; i >= 0; i-- {
			if cap(bufs[i].B) < need {
				continue
			}
			b := bufs[i]
			copy(bufs[i:], bufs[i+1:])
			p.idle[idx] = bufs[:len(bufs)-1]
			p.idleBuffers--
			p.retainedBytes -= int64(cap(b.B))
			p.leased[b] = struct{}{}
			p.stats.hits++
			return b
		}
	}

	b := &ByteBuffer{B: make([]byte, 0, allocSize)}
	p.leased[b] = struct{}{}
	p.stats.allocations++
	return b
}

// waitForLeaseLocked waits until a lease slot is available or the pool is
// closed, and reports whether a slot was obtained. p.mu must be held.
func (p *BoundedPool) waitForLeaseLocked() bool {
	waiting := false
	for !p.closed && p.cfg.MaxLeasedBuffers > 0 && len(p.leased) >= p.cfg.MaxLeasedBuffers {
		if !waiting {
			p.stats.blockedGets++
			waiting = true
		}
		p.cond.Wait()
	}
	if p.closed {
		p.stats.closedGets++
		return false
	}
	return true
}

// Put returns a byte buffer obtained from Get or GetSize to the pool.
//
// The buffer must not be accessed after Put. Put verifies that b is an
// outstanding lease of this pool; nil, duplicate and foreign returns are
// rejected with ReleaseRejected, leave the pool unchanged, and are not zeroed.
// A buffer that fits the configured limits is zeroed over its whole capacity
// and retained; otherwise it is dropped.
//
// Put never blocks. After Close, outstanding leases are still accepted, are
// reported as ReleaseReleased, and are not retained.
func (p *BoundedPool) Put(b *ByteBuffer) ReleaseStatus {
	p.init()

	p.mu.Lock()
	defer p.mu.Unlock()

	if b == nil {
		p.stats.rejectedPuts++
		return ReleaseRejected
	}
	if _, ok := p.leased[b]; !ok {
		p.stats.rejectedPuts++
		return ReleaseRejected
	}
	delete(p.leased, b)

	// A returned lease frees a slot for Get blocked at MaxLeasedBuffers. The
	// slot is free even if the buffer itself is dropped below.
	p.cond.Signal()

	if p.closed {
		p.stats.closedReleases++
		return ReleaseReleased
	}

	// Record the size the caller actually produced before the buffer is reset,
	// whether or not it ends up retained. Calibration is driven by usage.
	p.noteCallLocked(index(len(b.B)))

	size := cap(b.B)
	switch {
	case size > maxSize:
		p.stats.oversizeDrops++
		return ReleaseDropped
	case size < minSize:
		// A buffer whose capacity is below the smallest class cannot be
		// assigned to a class without changing its capacity, which would
		// change the slice the caller handed over. Drop it instead of
		// corrupting class accounting.
		p.stats.undersizeDrops++
		return ReleaseDropped
	}

	idx := index(size)
	if p.cfg.MaxBuffersPerClass > 0 && len(p.idle[idx]) >= p.cfg.MaxBuffersPerClass {
		p.stats.overLimitDrops++
		return ReleaseDropped
	}
	if p.cfg.MaxBytes > 0 && p.retainedBytes+int64(size) > p.cfg.MaxBytes {
		p.stats.overLimitDrops++
		return ReleaseDropped
	}

	// Zero the whole capacity, not just the used prefix: a reused buffer must
	// not expose bytes written by its previous user, including bytes past the
	// previous length.
	b.B = b.B[:size]
	zeroBytes(b.B)
	p.stats.zeroedBytes += uint64(size)
	b.B = b.B[:0]

	p.idle[idx] = append(p.idle[idx], b)
	p.idleBuffers++
	p.retainedBytes += int64(size)
	p.stats.retainedPuts++

	return ReleaseRetained
}

// Close closes the pool. It is safe to call multiple times.
//
// Close releases all idle buffers, wakes Get calls waiting at the
// MaxLeasedBuffers limit and makes subsequent Get and GetSize calls return
// nil. Buffers still leased remain usable; returning them via Put releases
// them without retaining them. Close never reclaims a buffer that is still in
// use by a caller.
func (p *BoundedPool) Close() {
	p.init()

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true
	for i := range p.idle {
		p.idle[i] = nil
	}
	p.idleBuffers = 0
	p.retainedBytes = 0
	p.cond.Broadcast()
}

// Closed reports whether Close has been called.
func (p *BoundedPool) Closed() bool {
	p.init()

	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// Stats returns a consistent snapshot of pool counters and inventory.
func (p *BoundedPool) Stats() BoundedPoolStats {
	p.init()

	p.mu.Lock()
	defer p.mu.Unlock()

	return BoundedPoolStats{
		Hits:            p.stats.hits,
		Allocations:     p.stats.allocations,
		RetainedPuts:    p.stats.retainedPuts,
		OverLimitDrops:  p.stats.overLimitDrops,
		OversizeDrops:   p.stats.oversizeDrops,
		UndersizeDrops:  p.stats.undersizeDrops,
		RejectedPuts:    p.stats.rejectedPuts,
		ClosedReleases:  p.stats.closedReleases,
		ClosedGets:      p.stats.closedGets,
		BlockedGets:     p.stats.blockedGets,
		Calibrations:    p.stats.calibrations,
		ZeroedBytes:     p.stats.zeroedBytes,
		IdleBuffers:     p.idleBuffers,
		RetainedBytes:   p.retainedBytes,
		LeasedBuffers:   len(p.leased),
		DefaultCapacity: classSize(p.defaultClass),
	}
}

// noteCallLocked records a Put for calibration and recalibrates once enough
// calls have been seen. Calibration only changes the class used by future Get
// calls; Put routing is always recomputed from the buffer capacity, so
// calibration can never move a buffer into another class. p.mu must be held.
func (p *BoundedPool) noteCallLocked(usedIdx int) {
	p.calls[usedIdx]++
	p.putsSinceCalibration++
	if p.putsSinceCalibration >= calibrateCallsThreshold {
		p.calibrateLocked()
	}
}

// calibrateLocked selects the most frequently used capacity class as the
// default class for Get. Ties resolve in favor of the smaller class. p.mu must
// be held.
func (p *BoundedPool) calibrateLocked() {
	bestClass := 0
	var bestCalls uint64
	var total uint64
	for i := range p.calls {
		calls := p.calls[i]
		p.calls[i] = 0
		total += calls
		if calls > bestCalls {
			bestCalls = calls
			bestClass = i
		}
	}
	p.putsSinceCalibration = 0
	p.stats.calibrations++
	if total > 0 {
		p.defaultClass = bestClass
	}
}

func classSize(idx int) int {
	return minSize << uint(idx)
}

// zeroBytes overwrites every byte of p with zero. The compiler lowers this
// loop to a runtime memclr call.
func zeroBytes(p []byte) {
	for i := range p {
		p[i] = 0
	}
}
