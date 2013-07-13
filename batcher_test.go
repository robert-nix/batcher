package batcher

import (
  "testing"
  "time"
)

type collTest struct {
  data       []byte
  numFlushes int
}

func (r *collTest) Add(e interface{}) {
  r.data = append(r.data, e.(byte))
}

func (r *collTest) Flush() {
  r.numFlushes++
  r.data = make([]byte, 0)
}

func (r *collTest) Count() int {
  return len(r.data)
}

func TestNew(t *testing.T) {
  c := &collTest{make([]byte, 0), 0}
  New(c, TimeInterval|ElementCountThreshold)
  <-time.After(1 * time.Millisecond)
  if c.numFlushes != 0 {
    t.Error("Flush was called by New")
  }
}

func TestFlush(t *testing.T) {
  c := &collTest{make([]byte, 0), 0}
  b := New(c, TimeInterval|ElementCountThreshold)
  b.Flush()
  if c.numFlushes != 1 {
    t.Errorf("Expected 1 Flush call, got %d", c.numFlushes)
  }
}

func TestAdd(t *testing.T) {
  c := &collTest{make([]byte, 0), 0}
  b := New(c, TimeInterval|ElementCountThreshold)
  b.Add(byte(0))
  b.Add(byte(0))
  if c.numFlushes != 0 {
    t.Error("Flush was called by unthresholded Add")
  }
  if c.Count() != 2 {
    t.Errorf("Expected Count = 2 after Add, got %d", c.Count())
  }
  b.SetThreshold(2)
  // SetThreshold should call Flush async since the Flush is "indirect"
  <-time.After(1 * time.Millisecond)
  if c.numFlushes != 1 {
    t.Error("Flush not called by SetThreshold")
  }
  b.Add(byte(1))
  b.Add(byte(1))
  if c.numFlushes != 2 {
    t.Error("Flush not called by thresholded Add")
  }
}

func TestSetInterval(t *testing.T) {
  c := &collTest{make([]byte, 0), 0}
  b := New(c, TimeInterval|ElementCountThreshold)
  b.SetInterval(1 * time.Millisecond)
  <-time.After(1500 * time.Microsecond)
  if c.numFlushes != 1 {
    t.Error("Flush not called on interval")
  }
  b.Destroy()
}

func TestDestroy(t *testing.T) {
  c := &collTest{make([]byte, 0), 0}
  b := New(c, TimeInterval|ElementCountThreshold)
  b.SetInterval(1 * time.Millisecond)
  b.Destroy()
  <-time.After(1500 * time.Microsecond)
  if c.numFlushes != 0 {
    t.Error("Flush called on dead interval")
  }
}
