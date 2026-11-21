// Package bytebufferpool implements a pool of byte buffers
// with anti-fragmentation protection.
//
// The pool may waste limited amount of memory due to fragmentation.
// This amount equals to the maximum total size of the byte buffers
// in concurrent use.
//
// Pool and the package-level Get and Put keep buffers by capacity class and
// calibrate which classes are worth keeping from observed traffic. BoundedPool
// is a closable alternative for long-running processes: it keeps a fixed
// maximum number of buffers per capacity class plus a total byte budget, drops
// buffers that do not fit, zeroes buffers before reuse, rejects duplicate and
// foreign returns, and never reuses storage that is still leased by a caller.
package bytebufferpool
