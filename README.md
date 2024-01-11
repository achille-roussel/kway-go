# kway-go [![Go Reference](https://pkg.go.dev/badge/github.com/achille-roussel/kway-go.svg)](https://pkg.go.dev/github.com/achille-roussel/kway-go)
K-way merge with Go 1.22 range functions

[bboreham]: https://github.com/bboreham
[godoc]: https://pkg.go.dev/github.com/achille-roussel/kway-go@v0.2.0#pkg-examples
[gophercon]: https://www.gophercon.com/agenda/session/1160355
[go1.22rc1]: https://gist.github.com/achille-roussel/5a9afe81c91891de4fad0bfe0965a9ea

## Installation

This package is intended to be used as a library and installed with:
```sh
go get github.com/achille-roussel/kway-go
```

:warning: The package depends on Go 1.22 (currently in rc1 release) and
enabling the rangefunc experiment.

To download Go 1.22 rc1: https://pkg.go.dev/golang.org/dl/go1.22rc1
```
go install golang.org/dl/go1.22rc1@latest
go1.22rc1 download
```
Then to enable the rangefunc experiment, set the GOEXPERIMENT environment
variable in the shell that executes the go commands:
```sh
export GOEXPERIMENT=rangefunc
```

For a more detailed guide of how to configure Go 1.22 with range functions see
[Go 1.22 rc1 installation to enable the range functions experiment][go1.22rc1].

## Usage

The package contains variations of the K-way merge algorithm for different
forms of iterator sequences:

* **Merge** and **MergeFunc** operate on sequences that yield single
  values. **Merge** must be used on ordered values, while **MergeFunc**
  accepts a comparison function as first argument to customize the
  ordering logic.

* **MergeSlice** and **MergeSliceFunc** are similar functions but operate on
  sequences that yield slices of values. These are intended for applications
  with higher throughput requirements that use batching or read values from
  paging APIs.

The sequences being merged must each be ordered using the same comparison logic
than the one used for the merge, or the algorithm will not be able to produce an
ordered sequence of values.

The following code snippets illustrates how to merge three ordered sequences
into one:
```go
for value := range kway.Merge(seq0, seq1, seq2) {
  ...
}
```

More examples are available in the [Go doc][godoc].

## Implementation

The K-way merge algorithm was inspired by the talk from
[Bryan Boreham][bboreham] at [Gophercon 2023][gophercon], which described
how using a loser-tree instead of a min-heap improved performance of Loki's
merge of log records.

The `kway-go` package also adds a specialization for cases where the program
is merging exactly two sequences, since this can be implemented as a simple
union of two sets which has a much lower compute and memory footprint.

## Performance

K-way merge is often used in stream processing or database engines to merge
distributed query results into a single ordered result set. In those
applications, performance of the underlying algorithms tend to matter: for
example, when performing compaction of sorted records, the merge algorithm is
on the critical path and often where most of the compute is being spent. In that
regard, there are efficiency requirements that the implementation must fulfil to
be a useful solution to those problems.

> :bulb: While exploring the performance characteristics of the algorithm, it is
> important to keep in mind that absolute numbers are only useful in the context
> where they were collected, since measurements depend on the hardware executing
> the code, and the data being processed. We should use relative performance of
> different benchmarks within a given context as a hint to find opportunities
> for optimizations in production applications, not as universal truths.

The current implementation has already been optimized to maximize throughput, by
amortizing as much of the baseline costs as possible, and ensure that CPU time is
spent on the important parts of the algorithm.

As part of this optimization work, it became apparent that while the Go runtime
implementation of coroutines underneath `iter.Pull2` has a much lower compute
footprint than using channels, it still has a significant overhead when reading
values in tight loops of the merge algorithm.

This graph shows a preview of the results, the full analysis is described in the
following sections:

![image](https://github.com/achille-roussel/kway-go/assets/865510/730da27c-e639-4cfe-878a-9cc5c9287e37)


### Establishing a performance baseline

To explore performance, let's first establish a baseline. We use the throughput
of merging a single sequence, which is simple reading all the values it yields
as comparison point:
```
Merge1  592898557  1.843 ns/op  0 comp/op   542741115 merge/s
```
This benchmark shows that on this test machine, the highest theoretical
throughput we can achieve is **~540M merge/s** for one sequence,
**~270M merge/s** when merging two sequences, etc...

### Performance analysis of the K-way merge algorithm

Now comparing the performance of merging two and three sequences:
```
Merge2   47742177  24.78 ns/op  0.8125 comp/op  40359389 merge/s
Merge3   27540648  42.23 ns/op  1.864 comp/op   23682342 merge/s
```
We observe a significant drop in throughput in comparison with iterating over
a single sequence, with the benchmark now performing **~7x slower** than the
theoretical throughput limit.

The K-way merge algorithm has a complexity of *O(n∙log(k))*, there would also be
a baseline cost for the added code implementing the merge operations, but almost
an order of magnitude difference seems unexpected.

To understand what is happening, we can look into a CPU profile:
```
Duration: 3.46s, Total samples = 2.44s (70.45%)
Showing nodes accounting for 2.40s, 98.36% of 2.44s total
Dropped 9 nodes (cum <= 0.01s)
 flat  flat%   sum%    cum   cum%
0.30s 12.30% 12.30%  0.72s 29.51%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].merge2[go.shape.int].func3
0.25s 10.25% 22.54%  0.34s 13.93%  github.com/achille-roussel/kway-go.(*tree[go.shape.int]).next
0.21s  8.61% 31.15%  0.76s 31.15%  github.com/achille-roussel/kway-go.sequence.func1
0.17s  6.97% 38.11%  0.26s 10.66%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].unbuffer[go.shape.int].func6.1
0.15s  6.15% 44.26%  0.25s 10.25%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func1.1
0.15s  6.15% 50.41%  0.21s  8.61%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func4.1
0.14s  5.74% 56.15%  0.23s  9.43%  iter.Pull2[go.shape.[]go.shape.int,go.shape.interface { Error string }].func2
0.13s  5.33% 61.48%  0.13s  5.33%  runtime/internal/atomic.(*Uint32).CompareAndSwap (inline)
0.11s  4.51% 65.98%  0.18s  7.38%  iter.Pull2[go.shape.[]go.shape.int,go.shape.interface { Error string }].func1.1
0.10s  4.10% 70.08%  0.27s 11.07%  runtime.coroswitch_m
0.09s  3.69% 73.77%  0.09s  3.69%  github.com/achille-roussel/kway-go.benchmark[go.shape.int].func2
0.09s  3.69% 77.46%  0.09s  3.69%  runtime.coroswitch
0.08s  3.28% 80.74%  0.11s  4.51%  gogo
0.07s  2.87% 83.61%  0.09s  3.69%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func2.1
0.06s  2.46% 86.07%  0.06s  2.46%  runtime.mapaccess1_fast64
0.05s  2.05% 88.11%  0.09s  3.69%  github.com/achille-roussel/kway-go.benchmark[go.shape.int].func1
0.04s  1.64% 89.75%  0.04s  1.64%  cmp.Compare[go.shape.int] (inline)
0.04s  1.64% 91.39%  0.04s  1.64%  internal/race.Acquire
0.04s  1.64% 93.03%  0.04s  1.64%  runtime.(*guintptr).cas (inline)
0.04s  1.64% 94.67%  0.32s 13.11%  runtime.mcall
0.04s  1.64% 96.31%  0.04s  1.64%  runtime.save_g
0.02s  0.82% 97.13%  0.02s  0.82%  internal/race.Release
0.01s  0.41% 97.54%  0.43s 17.62%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].merge[go.shape.int].func5
0.01s  0.41% 97.95%  0.04s  1.64%  github.com/achille-roussel/kway-go.nextNonEmptyValues[go.shape.int]
0.01s  0.41% 98.36%  0.08s  3.28%  runtime/pprof.(*profMap).lookup
    0     0% 98.36%  0.72s 29.51%  github.com/achille-roussel/kway-go.BenchmarkMerge2
    0     0% 98.36%  0.43s 17.62%  github.com/achille-roussel/kway-go.BenchmarkMerge3
    0     0% 98.36%  0.35s 14.34%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func1
    0     0% 98.36%  0.14s  5.74%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func2
    0     0% 98.36%  0.27s 11.07%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func4
    0     0% 98.36%  1.15s 47.13%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].unbuffer[go.shape.int].func6
    0     0% 98.36%  1.15s 47.13%  github.com/achille-roussel/kway-go.benchmark[go.shape.int]
    0     0% 98.36%  0.76s 31.15%  iter.Pull2[go.shape.[]go.shape.int,go.shape.interface { Error string }].func1
    0     0% 98.36%  0.76s 31.15%  runtime.corostart
```
As we can see here, a significant amount of time seems to be spent in the Go
runtime code managing coroutines. While it might be possible to optimize the
runtime, there is a lower bound on how much it can be reduced.

It is also unlikely that the Go compiler could help here, there are no real
opportunities for inlining or other optimizations.

### Performance optimization of the K-way merge algorithm

We basically have a very high baseline cost for each operation, with the
hypothesis that it is driven by coroutine context switch implemented in the
runtime, the only thing we can do to improve performance is doing less of these.

This is a typical a baseline cost amortization problem: we want to call the
`next` function returned by `iter.Pull2` less often, which can be done by
introducing buffering. Instead of pulling values one at a time, we can
efficiently buffer N values from each sequence in memory, by transposing
the `iter.Seq2[T, error]` sequences into `iter.Seq2[[]T, error]`. The call
to `next` then only needs to happen when we exhaust the buffer, which ends up
amortizing its cost.

With an internal buffer size of **128** values per sequence:
```
Merge2  190103247  6.133 ns/op  0.8333 comp/op  163045156 merge/s
Merge3  95485022  12.74 ns/op   1.864 comp/op    78492807 merge/s
```
Now we made the algorithm **3-4x faster**, and have performance in the range of
**1.5 to 2.5x** the theoretical throughput limit.

It is interesting to note that the CPU profile didn't seem to indicate that 75%
of the time was spent in the runtime, but reducing the time spent in that code
path has had a non-linear impact on performance. Likely some other CPU
instruction pipeline and caching shenanigans are at play here, possibly impacted
by the atomic compare-and-swap operations in coroutine switches.

As expected, the CPU profile now shows that almost no time is spent in the
runtime:
```
Duration: 3.17s, Total samples = 2.35s (74.08%)
Showing nodes accounting for 2.28s, 97.02% of 2.35s total
Dropped 22 nodes (cum <= 0.01s)
 flat  flat%   sum%    cum   cum%
0.45s 19.15% 19.15%  0.56s 23.83%  github.com/achille-roussel/kway-go.(*tree[go.shape.int]).next
0.43s 18.30% 37.45%  0.43s 18.30%  github.com/achille-roussel/kway-go.benchmark[go.shape.int].func2
0.37s 15.74% 53.19%  0.97s 41.28%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].merge2[go.shape.int].func3
0.23s  9.79% 62.98%  0.24s 10.21%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func1.1
0.22s  9.36% 72.34%  0.65s 27.66%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].unbuffer[go.shape.int].func6.1
0.13s  5.53% 77.87%  0.13s  5.53%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func4.1
0.12s  5.11% 82.98%  0.21s  8.94%  github.com/achille-roussel/kway-go.benchmark[go.shape.int].func1
0.10s  4.26% 87.23%  0.52s 22.13%  github.com/achille-roussel/kway-go.sequence.func1
0.09s  3.83% 91.06%  0.09s  3.83%  cmp.Compare[go.shape.int] (inline)
0.05s  2.13% 93.19%  0.05s  2.13%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func2.1
0.03s  1.28% 94.47%  0.06s  2.55%  runtime/pprof.(*profMap).lookup
0.02s  0.85% 95.32%  0.02s  0.85%  github.com/achille-roussel/kway-go.parent (inline)
0.02s  0.85% 96.17%  0.02s  0.85%  runtime.asyncPreempt
0.02s  0.85% 97.02%  0.02s  0.85%  runtime.mapaccess1_fast64
    0     0% 97.02%  0.97s 41.28%  github.com/achille-roussel/kway-go.BenchmarkMerge2
    0     0% 97.02%  0.76s 32.34%  github.com/achille-roussel/kway-go.BenchmarkMerge3
    0     0% 97.02%  0.31s 13.19%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func1
    0     0% 97.02%  0.08s  3.40%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func2
    0     0% 97.02%  0.13s  5.53%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].buffer[go.shape.int].func4
    0     0% 97.02%  0.76s 32.34%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].merge[go.shape.int].func5
    0     0% 97.02%  1.73s 73.62%  github.com/achille-roussel/kway-go.MergeFunc[go.shape.int].unbuffer[go.shape.int].func6
    0     0% 97.02%  1.73s 73.62%  github.com/achille-roussel/kway-go.benchmark[go.shape.int]
    0     0% 97.02%  0.52s 22.13%  iter.Pull2[go.shape.[]go.shape.int,go.shape.interface { Error string }].func1
    0     0% 97.02%  0.52s 22.13%  runtime.corostart
```

### Further optimizations using batch processing

There is a final performance frontier we can cross. While we are buffering
values internally, the input and output sequences remain `iter.Seq2[T, error]`,
which yield values one by one. Often times in data systems, APIs have pagination
capabilities, or stream processors work on batch of values for the same reason
we added buffering: it reduces the baseline cost of crossing system boundaries.

If the input sequences are already slices of values, and the output sequence
produces slices of values, we can reduce the internal memory footprint (no need
to allocate memory to buffer the inputs), while also further amortizing the cost
of function calls to yield values in and out of the merge algorithm.

Applications that fall into those categories can unlock further performance by
using `MergeSlice` instead of `Merge`, which works on `iter.Seq2[[]T, error]`
end-to-end.

What is interesting with this approach is that in cases where the processing of
inputs and outputs can be batched, this model **can even beat the theoretical
throughput limit**. For example, in the benchmarks we've used, the body of the
loop consuming merged values simply counts the results. When consuming slices
there is no need to iterate over the slices and increment the counter by one
each time, we can batch the operation by incrementing the counter by the length
of the slice, achieving much higher throughput than predicted by the baseline:
```
MergeSlice2  477720793  2.273 ns/op  0.6688 comp/op  439971259 merge/s
MergeSlice3  150406080  7.945 ns/op  1.667 comp/op   125861613 merge/s
```

> :warning: Keep in mind that to minimize the footprint, `MergeSlice` resuses
> its output buffer, which means that the application cannot retain it beyond
> the body of the loop raning over the merge function. This can lead to subtle
> bugs that can be difficult to track, `Merge` should always be preferred unless
> there is clear evidence that the increased maintenance cost is worth the
> performance benefits.
