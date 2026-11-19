# ecache2 v0.1.1: shard-lock layout benchmark

Compared with published v0.1.0, the only runtime change is the shard-lock layout. Independent shard mutexes are now separated by an architecture-sized, power-of-two stride. APIs, hashing, LRU/LRU-2, expiration and inspector behavior are unchanged. Go 1.22 remains supported.

## Change

```go
const cacheLineSize = max(64, unsafe.Sizeof(cpu.CacheLinePad{}))

type cacheMutex struct {
    sync.Mutex
    _ [cacheLineSize - unsafe.Sizeof(sync.Mutex{})]byte
}
```

The lock slice and its allocation use `[]cacheMutex`. `unsafe` is used only for compile-time sizes, with no pointer operations. `x/sys/cpu` was already a transitive dependency; its version is unchanged. The 64-byte minimum also permits targets such as WebAssembly where CacheLinePad has size zero.

Subtracting the mutex size avoids the experimental 136-byte stride on arm64: the final stride is 128 bytes there and 64 bytes on amd64. The distance keeps independent locks from sharing a cache line; it does not remove contention when callers use the same shard. This increases lock storage by 120 bytes per shard on arm64: 1,920 bytes for 16 shards, excluding allocator rounding.

## Results

On Apple M4 Pro / macOS 26.6.2 / Go 1.27.4, random reads across 256 preloaded string keys improved from **67.87 to 24.45 ns/op at 4 CPUs (2.78×)** and **119.90 to 45.99 ns/op at 8 CPUs (2.61×)**. All measured hit paths allocate **0 B/op and 0 allocs/op**.

The single-CPU integer-key sample increased from 10.26 to 10.83 ns/op (about 5.6% or 0.57 ns). Single-CPU pair-key and random-key samples were also slightly slower. These results are retained below; this change does not make every workload faster.

Five paired runs alternate baseline/candidate order. The table gives median [minimum, maximum] ns/op. A missing CPU suffix means GOMAXPROCS=1. Ratios greater than one favor the candidate. Timing runs did not overlap tests or builds.

| Benchmark | v0.1.0 median [min, max] | v0.1.1 median [min, max] | Old/new |
| --- | ---: | ---: | ---: |
| Distributed | 22.030 [21.860, 22.490] | 22.230 [22.060, 23.210] | 0.99× |
| Distributed-4 | 67.870 [56.270, 79.000] | 24.450 [23.220, 30.090] | 2.78× |
| Distributed-8 | 119.900 [116.600, 122.100] | 45.990 [44.980, 47.090] | 2.61× |
| IntegerShards | 10.260 [10.150, 10.650] | 10.830 [10.540, 11.100] | 0.95× |
| IntegerShards-4 | 54.790 [54.160, 60.850] | 2.865 [2.755, 2.978] | 19.12× |
| IntegerShards-8 | 74.100 [72.340, 78.920] | 1.749 [1.659, 1.991] | 42.37× |
| PairShards | 14.130 [13.930, 14.440] | 14.410 [14.310, 14.820] | 0.98× |
| PairShards-4 | 68.660 [66.680, 72.170] | 3.958 [3.870, 4.412] | 17.35× |
| PairShards-8 | 76.010 [75.110, 89.630] | 2.385 [2.350, 2.609] | 31.87× |
| Serial | 14.550 [14.030, 15.470] | 14.190 [14.110, 14.630] | 1.03× |
| Serial-4 | 14.300 [14.080, 14.940] | 14.130 [14.090, 14.580] | 1.01× |
| Serial-8 | 14.280 [14.020, 15.780] | 14.220 [14.080, 14.380] | 1.00× |
| Shards | 15.010 [14.910, 23.710] | 14.900 [14.850, 15.140] | 1.01× |
| Shards-4 | 69.800 [66.320, 75.990] | 3.970 [3.916, 4.089] | 17.58× |
| Shards-8 | 79.860 [78.180, 84.320] | 2.426 [2.351, 2.606] | 32.92× |
| Shared | 15.010 [14.840, 15.650] | 14.950 [14.830, 15.380] | 1.00× |
| Shared-4 | 151.000 [148.500, 163.000] | 120.400 [117.300, 122.800] | 1.25× |
| Shared-8 | 124.500 [118.000, 126.100] | 117.400 [116.900, 118.000] | 1.06× |

`Serial` repeatedly reads one key in one goroutine. `Shared` uses RunParallel to read the same key. `Shards` gives workers keys in distinct adjacent shards, isolating false sharing from true lock contention. `Distributed` randomly reads 256 preloaded string keys using independent per-worker PRNG state. IntegerShards and PairShards, where present, exercise ecache2 integer and two-integer keys on distinct shards. All caches use 16 shards, capacity 64 per shard, and a one-minute sliding TTL; setup and string formatting occur before timing. Every returned value contributes to a sum stored to an atomic sink after the loop.

These are hit-path microbenchmarks, not application-throughput or memory-footprint measurements. Independent-shard results deliberately isolate the contention problem. Filesystem I/O, misses, eviction, writes and other architectures were not performance-benchmarked.

## Reproduce

Take the baseline tag and candidate source in separate directories, keeping their own go.mod/go.sum. Put the same [cache_bench_test.go](../../cache_bench_test.go) in each, then compile in the corresponding directories:

```sh
GOTOOLCHAIN=go1.27.4 go test -mod=readonly -c -o /tmp/cache-VARIANT.test
```

Run the two binaries alternately five times, reversing order every other round:

```sh
/tmp/cache-VARIANT.test -test.run='^$' -test.bench='^BenchmarkGet' -test.benchmem -test.benchtime=300ms -test.cpu=1,4,8 -test.count=1
```

Raw outputs: [baseline](baseline.txt), [candidate](candidate.txt). Parsed observations: [measurements.json](measurements.json). Exact inputs, binary SHA-256, environment, commands and order: [metadata.json](metadata.json).

## Validation

- All original local cache, stats and in-process distribution tests passed, including the million-iteration concurrency cases: `go test -mod=readonly -count=1 -timeout=4m . ./stats ./dist`.
- Core functional tests plus all benchmark patterns passed race checks at CPU settings 1/4/8 and 1,000 iterations per pattern. Race timings are excluded from performance results.
- Go 1.22.12 core race checks passed using external linking on macOS; the existing LC_DYSYMTAB linker warning remains.
- Go 1.22.12 library builds passed for Linux amd64, Linux arm64 and JavaScript/WebAssembly; these were compile checks, not runtime performance tests.
- All three Redis adapters passed their original tests against an isolated local Redis server: `go test -mod=readonly -p=1 -count=1 -timeout=2m ./dist/goredis/... ./dist/redigo`. [Adapter outputs](redis-tests.txt).
- `go vet ./...`, module verification and diff whitespace checks passed.

The full suite was not run with race instrumentation; distribution helpers retain their existing synchronization behavior.
