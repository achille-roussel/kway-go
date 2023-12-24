# kway-go
K-way merge with Go 1.22 range functions

## Installation

This package is intended to be used as a library and installed with:
```sh
go get github.com/achille-roussel/kway-go
```

## Examples

The package contains variations of the k-way merge algorithm for different
forms of iterator sequences:

* **Merge** and **MergeFunc** operate on sequences that yield single
  values. **Merge** must be used on ordered values, while **MergeFunc**
  accepts a comparison function as first argument to customize the
  ordering logic.

* **Merge2** and **Merge2Func** are similar functions but operate on
  sequences that yield pairs of values.

The sequences being merged must each be locally ordered using the same
comparison logic than the one used for the merge, or the algorithm will not
be able to produce an ordered sequence of values.

The following code snipets illustrates how to merge three ordered sequences
into one:
```go
for value := range kway.Merge(seq0, seq1, seq2) {
  ...
}
```

## Implementation

The k-way merge algorithm uses different implementations depending on the
input sequences being merged. For example, when merging two sequences, it
combines them using an optimized merge which acts like an intersection
algorithm. When merging more than two sequences, the implementation uses
a min-heap to merge all the incoming sequences. The heap algorithm is a
simplified version of the standard library's `container/heap` package.

### High-performance merges

### Note on Heap vs Loser Tree

There is an experiment of replacing the min-heap with a loser tree on the
[loser-tree](https://github.com/achille-roussel/kway-go/tree/loser-tree)
branch. In theory, the loser tree implementation can achieve up to 50% better
throughput compared to the min-heap because it does a single pass through the
tree of *k* sequences to merge, instead of the two passes that the heap usually
requires, which results in less comparisons overall.

However, I wasn't able to craft a benchmark that would show the loser tree
version performing better than the min-heap; in all cases, the heap gives equal
or better throughput, even when it has to do more comparisons. It is possible
that production data would show different results, but until then the heap
implementation is a safer and simpler solution.
