package dal

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.opentelemetry.io/otel/metric"
)

// IOPSCounters holds atomic counters for I/O operations.
type IOPSCounters struct {
	ReadOps  atomic.Int64
	WriteOps atomic.Int64
	SyncOps  atomic.Int64
}

// RegisterMetrics registers OTEL observable gauges that report IOPS counters.
func (c *IOPSCounters) RegisterMetrics(m metric.Meter) (metric.Registration, error) {
	readOps, err := m.Int64ObservableCounter(
		"pebble.vfs.read.ops",
		metric.WithDescription("Total VFS read operations"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating pebble.vfs.read.ops counter: %w", err)
	}

	writeOps, err := m.Int64ObservableCounter(
		"pebble.vfs.write.ops",
		metric.WithDescription("Total VFS write operations"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating pebble.vfs.write.ops counter: %w", err)
	}

	syncOps, err := m.Int64ObservableCounter(
		"pebble.vfs.sync.ops",
		metric.WithDescription("Total VFS sync operations"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating pebble.vfs.sync.ops counter: %w", err)
	}

	return m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(readOps, c.ReadOps.Load())
		o.ObserveInt64(writeOps, c.WriteOps.Load())
		o.ObserveInt64(syncOps, c.SyncOps.Load())

		return nil
	}, readOps, writeOps, syncOps)
}

// metricsFS wraps a vfs.FS and counts I/O operations via atomic counters.
type metricsFS struct {
	inner    vfs.FS
	counters *IOPSCounters
}

// NewMetricsFS wraps inner and counts read/write/sync operations in counters.
func NewMetricsFS(inner vfs.FS, counters *IOPSCounters) vfs.FS {
	return &metricsFS{inner: inner, counters: counters}
}

func (f *metricsFS) wrapFile(file vfs.File) vfs.File {
	return &metricsFile{inner: file, counters: f.counters}
}

func (f *metricsFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.inner.Create(name, category)
	if err != nil {
		return nil, err
	}

	return f.wrapFile(file), nil
}

func (f *metricsFS) Link(oldname, newname string) error {
	return f.inner.Link(oldname, newname)
}

func (f *metricsFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.inner.Open(name, opts...)
	if err != nil {
		return nil, err
	}

	return f.wrapFile(file), nil
}

func (f *metricsFS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.inner.OpenReadWrite(name, category, opts...)
	if err != nil {
		return nil, err
	}

	return f.wrapFile(file), nil
}

func (f *metricsFS) OpenDir(name string) (vfs.File, error) {
	file, err := f.inner.OpenDir(name)
	if err != nil {
		return nil, err
	}

	return f.wrapFile(file), nil
}

func (f *metricsFS) Remove(name string) error {
	return f.inner.Remove(name)
}

func (f *metricsFS) RemoveAll(name string) error {
	return f.inner.RemoveAll(name)
}

func (f *metricsFS) Rename(oldname, newname string) error {
	return f.inner.Rename(oldname, newname)
}

func (f *metricsFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.inner.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}

	return f.wrapFile(file), nil
}

func (f *metricsFS) MkdirAll(dir string, perm os.FileMode) error {
	return f.inner.MkdirAll(dir, perm)
}

func (f *metricsFS) Lock(name string) (io.Closer, error) {
	return f.inner.Lock(name)
}

func (f *metricsFS) List(dir string) ([]string, error) {
	return f.inner.List(dir)
}

func (f *metricsFS) Stat(name string) (vfs.FileInfo, error) {
	return f.inner.Stat(name)
}

func (f *metricsFS) PathBase(path string) string {
	return f.inner.PathBase(path)
}

func (f *metricsFS) PathJoin(elem ...string) string {
	return f.inner.PathJoin(elem...)
}

func (f *metricsFS) PathDir(path string) string {
	return f.inner.PathDir(path)
}

func (f *metricsFS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	return f.inner.GetDiskUsage(path)
}

func (f *metricsFS) Unwrap() vfs.FS {
	return f.inner
}

// metricsFile wraps a vfs.File and counts I/O operations.
type metricsFile struct {
	inner    vfs.File
	counters *IOPSCounters
}

func (f *metricsFile) Close() error {
	return f.inner.Close()
}

func (f *metricsFile) Read(p []byte) (int, error) {
	f.counters.ReadOps.Add(1)

	return f.inner.Read(p)
}

func (f *metricsFile) ReadAt(p []byte, off int64) (int, error) {
	f.counters.ReadOps.Add(1)

	return f.inner.ReadAt(p, off)
}

func (f *metricsFile) Write(p []byte) (int, error) {
	f.counters.WriteOps.Add(1)

	return f.inner.Write(p)
}

func (f *metricsFile) WriteAt(p []byte, off int64) (int, error) {
	f.counters.WriteOps.Add(1)

	return f.inner.WriteAt(p, off)
}

func (f *metricsFile) Preallocate(offset, length int64) error {
	return f.inner.Preallocate(offset, length)
}

func (f *metricsFile) Stat() (vfs.FileInfo, error) {
	return f.inner.Stat()
}

func (f *metricsFile) Sync() error {
	f.counters.SyncOps.Add(1)

	return f.inner.Sync()
}

func (f *metricsFile) SyncTo(length int64) (fullSync bool, err error) {
	f.counters.SyncOps.Add(1)

	return f.inner.SyncTo(length)
}

func (f *metricsFile) SyncData() error {
	f.counters.SyncOps.Add(1)

	return f.inner.SyncData()
}

func (f *metricsFile) Prefetch(offset, length int64) error {
	return f.inner.Prefetch(offset, length)
}

func (f *metricsFile) Fd() uintptr {
	return f.inner.Fd()
}
