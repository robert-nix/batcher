package batcher

import (
  "sync"
  "time"
)

type Collection interface {
  // Add adds an element to the Collection
  Add(e interface{})
  // Flush synchronously flushes the Collection
  Flush()
  // Count returns the element count of the Collection
  Count() int
}

// Buffer provides a wrapper for Collections to have a Flush method
// called on a timed interval or on reaching a set element count threshold.
// Buffer also wraps Flush and Add calls in a per-buffer mutex.
type Buffer struct {
  mutex          *sync.Mutex
  data           Collection
  mode           FlushMode
  threshold      int
  interval       time.Duration
  intervalUpdate chan bool
  stop           chan bool
}

type FlushMode int

const (
  Manual FlushMode = 1 << iota
  // A flush will trigger on an interval set with SetInterval.  This will create
  // a ticking goroutine.  An interval must be set with SetInterval; there is no
  // default.
  TimeInterval
  // A flush will trigger when the Collection's Count equals the threshold set
  // by SetThreshold.  A threshold must be set with SetThreshold; there is no
  // default.
  ElementCountThreshold
)

func New(data Collection, mode FlushMode) *Buffer {
  b := &Buffer{&sync.Mutex{}, data, mode, 0, 0, make(chan bool), make(chan bool)}
  if mode&TimeInterval != Manual {
    go func() {
      for {
        var tick <-chan time.Time
        if b.interval > 0 {
          tick = time.After(b.interval)
        } else {
          tick = make(chan time.Time)
        }

        select {
        case <-b.stop:
          return
        case <-b.intervalUpdate:
        case <-tick:
          b.Flush()
        }
      }
    }()
  }
  return b
}

func (b *Buffer) Flush() {
  b.mutex.Lock()
  defer b.mutex.Unlock()

  b.data.Flush()
}

func (b *Buffer) Add(e interface{}) {
  b.mutex.Lock()
  defer b.mutex.Unlock()

  b.data.Add(e)
  if b.threshold != 0 && b.data.Count() >= b.threshold {
    b.data.Flush()
  }
}

// SetInterval sets the interval for b.  The first flush will occur at time
// `interval` after SetInterval is called.
func (b *Buffer) SetInterval(interval time.Duration) (previous time.Duration) {
  previous = b.interval
  b.interval = interval
  go func() {
    b.intervalUpdate <- true
  }()
  return previous
}

// SetThreshold will trigger a flush if threshold is below the current element
// count.
func (b *Buffer) SetThreshold(threshold int) (previous int) {
  previous = b.threshold
  b.threshold = threshold
  if b.threshold != 0 && b.data.Count() >= b.threshold {
    go b.Flush()
  }
  return previous
}

// Destroy stops the ticking goroutine for Buffer b, which is required for
// releasing the resources given to the Buffer.
func (b *Buffer) Destroy() {
  if b.mode&TimeInterval != Manual {
    b.stop <- true
  }
}
