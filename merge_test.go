package kway

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
	"testing"
	"time"
)

//go:noinline
func bulkCount(n, r int) iter.Seq[[]int] {
	return func(yield func([]int) bool) {
		values := make([]int, r)
		for i := range n {
			n := i * r
			for j := range values {
				values[j] = n + j
			}
			if !yield(values) {
				return
			}
		}
	}
}

//go:noinline
func count(n int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := range n {
			if !yield(i) {
				return
			}
		}
	}
}

//go:noinline
func squares(n int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := range n {
			if !yield(i * i) {
				return
			}
		}
	}
}

//go:noinline
func sequence(min, max, step int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := min; i < max; i += step {
			if !yield(i) {
				return
			}
		}
	}
}

func values[T any](seq iter.Seq[T]) (values []T) {
	for v := range seq {
		values = append(values, v)
	}
	return values
}

func TestBulkMerge(t *testing.T) {
	for n := range 10 {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			seqs := make([]iter.Seq[[]int], n)
			want := make([]int, 0, 2*n)

			for i := range seqs {
				seqs[i] = bulkCount(i, 10)
				vs := values(count(i * 10))
				want = append(want, vs...)
			}

			slices.Sort(want)

			seq := BulkMerge(seqs...)
			got := values(seq)

			if !slices.Equal(got, want) {
				t.Errorf("expected %v, got %v", want, got)
			}
		})
	}
}

func TestMerge(t *testing.T) {
	for n := range 10 {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			seqs := make([]iter.Seq[int], n)
			want := make([]int, 0, 2*n)

			for i := range seqs {
				seqs[i] = count(i)
				vs := values(count(i))
				want = append(want, vs...)
			}

			slices.Sort(want)

			seq := Merge(seqs...)
			got := values(seq)

			if !slices.Equal(got, want) {
				t.Errorf("expected %v, got %v", want, got)
			}
		})
	}
}

func BenchmarkBulkMergeOne(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq[int] {
		return BulkMergeFunc(cmp, bulkCount(n/10, 10))
	})
}

func BenchmarkBulkMergeTwo(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq[int] {
		const N1 = 100
		const N2 = 128
		n1 := n / (2 * N1)
		n2 := n / (2 * N2)
		return BulkMergeFunc(cmp, bulkCount(n1, N1), bulkCount(n2, N2))
	})
}

func BenchmarkMergeOne(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq[int] {
		return MergeFunc(cmp, count(n))
	})
}

func BenchmarkMergeTwo(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq[int] {
		n1 := n / 3
		n2 := n - n1
		return MergeFunc(cmp, count(n1), squares(n2))
	})
}

func BenchmarkMergeThree(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq[int] {
		return MergeFunc(cmp,
			sequence(0, 1*n/3, 1),
			sequence(0, 2*n/3, 2),
			sequence(0, 3*n/3, 3),
		)
	})
}

func benchmark[V cmp.Ordered](b *testing.B, merge func(int, func(V, V) int) iter.Seq[V]) {
	comparisons := 0
	compare := func(a, b V) int {
		comparisons++
		return cmp.Compare(a, b)
	}
	start := time.Now()
	count := b.N
	for _ = range merge(count, compare) {
		if count--; count == 0 {
			break
		}
	}
	duration := time.Since(start)
	b.ReportMetric(float64(b.N)/duration.Seconds(), "merge/s")
	b.ReportMetric(float64(comparisons)/float64(b.N), "comp/op")
}
