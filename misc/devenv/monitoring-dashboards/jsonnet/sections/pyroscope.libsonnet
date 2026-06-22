// Profiling (Pyroscope) section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';

panels.row('Profiling (Pyroscope)', 174, [
  panels.flamegraph(
    'CPU Flame Graph',
    { h: 20, w: 24, x: 0, y: 92 },
    'process_cpu:cpu:nanoseconds:cpu:nanoseconds',
    description=|||
      CPU flame graph from Pyroscope continuous profiling. Shows where CPU time is spent in the application.
      
      Use the version tag to compare profiles between different builds.
   |||,
  ),

  panels.flamegraph(
    'Memory Allocation Flame Graph',
    { h: 20, w: 24, x: 0, y: 112 },
    'memory:alloc_space:bytes:space:bytes',
    description='Memory allocation flame graph from Pyroscope. Shows where memory is being allocated in the application.',
  ),

  panels.flamegraph(
    'Memory In-Use Flame Graph',
    { h: 20, w: 24, x: 0, y: 132 },
    'memory:inuse_space:bytes:space:bytes',
    description='Memory in-use flame graph from Pyroscope. Shows current memory usage in the application.',
  ),

  panels.flamegraph(
    'Goroutines Flame Graph',
    { h: 20, w: 24, x: 0, y: 152 },
    'goroutine:goroutine:count:goroutine:count',
    description='Goroutine flame graph from Pyroscope. Shows goroutine stack traces.',
  ),
])
