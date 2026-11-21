[![Build Status](https://travis-ci.org/valyala/bytebufferpool.svg)](https://travis-ci.org/valyala/bytebufferpool)
[![GoDoc](https://godoc.org/github.com/valyala/bytebufferpool?status.svg)](http://godoc.org/github.com/valyala/bytebufferpool)
[![Go Report](http://goreportcard.com/badge/valyala/bytebufferpool)](http://goreportcard.com/report/valyala/bytebufferpool)

# bytebufferpool

An implementation of a pool of byte buffers with anti-memory-waste protection.

The pool may waste limited amount of memory due to fragmentation.
This amount equals to the maximum total size of the byte buffers
in concurrent use.

# Bounded pool

`Pool` keeps buffers purely by capacity class and calibrates which classes are
worth keeping from observed traffic. A long-running proxy that occasionally
grows a buffer for a large response may therefore keep those large slices after
the burst is over, and small requests may receive storage grown by large ones.

`BoundedPool` is a closable alternative:

```go
pool := bytebufferpool.NewBoundedPool(bytebufferpool.BoundedPoolConfig{
	MaxBuffersPerClass: 8,      // fixed number of idle buffers per capacity class
	MaxBytes:           32 << 20, // total capacity of idle buffers
})

bb := pool.Get()      // zeroed lease from the calibrated class
defer pool.Put(bb)    // zeroes and retains, or releases over the limits

// ...

pool.Close()          // releases idle storage and wakes blocked Get calls
```

* `Get` and `GetSize` return zeroed buffers that are outstanding leases.
  `GetSize` takes the smallest class able to hold the requested size, so
  workloads of different sizes do not share slices.
* `Put` accepts only outstanding leases of that pool. Nil, duplicate and
  foreign buffers are rejected and leave the pool unchanged. A buffer is
  retained only while its class and the byte budget have room; otherwise it is
  dropped.
* Buffers leased before `Close` stay usable. Returning them releases them for
  good; the pool never reuses storage a caller still holds.
* `Close` is idempotent and wakes all waiting callers. `Stats` reports hits,
  allocations, drops by reason, rejected returns and current inventory.

The default `Get`, `Put` and the `ByteBuffer` interface are unchanged.

# Benchmark results
Currently bytebufferpool is fastest and most effective buffer pool written in Go.

You can find results [here](https://omgnull.github.io/go-benchmark/buffer/).

# bytebufferpool users

* [fasthttp](https://github.com/valyala/fasthttp)
* [quicktemplate](https://github.com/valyala/quicktemplate)
