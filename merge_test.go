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
func countSlice(n, r int) iter.Seq[[]int] {
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
func sequence(min, max, step int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := min; i < max; i += step {
			if !yield(i) {
				return
			}
		}
	}
}

func bulkValues[T any](seq iter.Seq[[]T]) (values []T) {
	for vs := range seq {
		values = append(values, vs...)
	}
	return values
}

func values[T any](seq iter.Seq[T]) (values []T) {
	for v := range seq {
		values = append(values, v)
	}
	return values
}

func TestMergeSlice(t *testing.T) {
	for n := range 10 {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			seqs := make([]iter.Seq[[]int], n)
			want := make([]int, 0, 2*n)

			for i := range seqs {
				seqs[i] = countSlice(i, 10)
				vs := values(count(i * 10))
				want = append(want, vs...)
			}

			slices.Sort(want)

			seq := MergeSlice(seqs...)
			got := bulkValues(seq)

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

func BenchmarkMergeSliceOne(b *testing.B) {
	benchmarkSlice(b, func(n int, cmp func(int, int) int) iter.Seq[[]int] {
		return MergeSliceFunc(cmp, countSlice(n, 10))
	})
}

func BenchmarkMergeSliceTwo(b *testing.B) {
	benchmarkSlice(b, func(n int, cmp func(int, int) int) iter.Seq[[]int] {
		return MergeSliceFunc(cmp, countSlice(n, 100), countSlice(n, 127))
	})
}

func BenchmarkMergeOne(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq[int] {
		return MergeFunc(cmp, count(n))
	})
}

func BenchmarkMergeTwo(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq[int] {
		return MergeFunc(cmp,
			sequence(0, n-(n/4), 1),
			sequence(n/4, n, 2),
		)
	})
}

func BenchmarkMergeThree(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq[int] {
		return MergeFunc(cmp,
			sequence(0, n, 2),
			sequence(n/4, n, 1),
			sequence(n/3, n, 3),
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
	if count != 0 {
		b.Fatalf("expected %d values, got %d", b.N, b.N-count)
	}
	duration := time.Since(start)
	b.ReportMetric(float64(b.N)/duration.Seconds(), "merge/s")
	b.ReportMetric(float64(comparisons)/float64(b.N), "comp/op")
}

func benchmarkSlice[V cmp.Ordered](b *testing.B, merge func(int, func(V, V) int) iter.Seq[[]V]) {
	comparisons := 0
	compare := func(a, b V) int {
		comparisons++
		return cmp.Compare(a, b)
	}
	start := time.Now()
	count := b.N
	for values := range merge(count, compare) {
		if count -= len(values); count <= 0 {
			break
		}
	}
	if count > 0 {
		b.Fatalf("expected %d values, got %d", b.N, b.N-count)
	}
	duration := time.Since(start)
	b.ReportMetric(float64(b.N)/duration.Seconds(), "merge/s")
	b.ReportMetric(float64(comparisons)/float64(b.N), "comp/op")
}
