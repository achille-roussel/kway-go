package kway

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
	"testing"
	"time"
)

func count(n int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := range n {
			if !yield(i) {
				return
			}
		}
	}
}

func squares(n int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := range n {
			if !yield(i * i) {
				return
			}
		}
	}
}

func count2(n int) iter.Seq2[int, int] {
	return func(yield func(int, int) bool) {
		for i := range n {
			if !yield(i, i) {
				return
			}
		}
	}
}

func squares2(n int) iter.Seq2[int, int] {
	return func(yield func(int, int) bool) {
		for i := range n {
			if !yield(i, i*i) {
				return
			}
		}
	}
}

func sequence(min, max, step int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := min; i < max; i += step {
			if !yield(i) {
				return
			}
		}
	}
}

func sequence2(min, max, step int) iter.Seq2[int, int] {
	return func(yield func(int, int) bool) {
		for i, j := 0, min; j < max; j += step {
			if !yield(i, j) {
				return
			}
			i++
		}
	}
}

func values[T any](seq iter.Seq[T]) (values []T) {
	for v := range seq {
		values = append(values, v)
	}
	return values
}

func TestMerge(t *testing.T) {
	test(t, Merge[int])
}

func test(t *testing.T, merge func(...iter.Seq[int]) iter.Seq[int]) {
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

			seq := merge(seqs...)
			got := values(seq)

			if !slices.Equal(got, want) {
				t.Errorf("expected %v, got %v", want, got)
			}
		})
	}
}

func BenchmarkMergeOne(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq[int] {
		return MergeFunc(cmp, count(n))
	})
}

func BenchmarkMergeTwo(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq[int] {
		return MergeFunc(cmp, count(n/2), squares(n/2))
	})
}

func BenchmarkMergeTen(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq[int] {
		return MergeFunc(cmp,
			sequence(0, 1*n/10, 1),
			sequence(1, 2*n/10, 2),
			sequence(2, 3*n/10, 3),
			sequence(3, 4*n/10, 4),
			sequence(4, 5*n/10, 5),
			sequence(5, 6*n/10, 6),
			sequence(6, 7*n/10, 7),
			sequence(7, 8*n/10, 8),
			sequence(8, 9*n/10, 9),
			sequence(9, 10*n/10, 10),
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

func BenchmarkMerge2One(b *testing.B) {
	benchmark2(b, func(n int, cmp func(int, int) int) iter.Seq2[int, int] {
		return Merge2Func(cmp, count2(n))
	})
}

func BenchmarkMerge2Two(b *testing.B) {
	benchmark2(b, func(n int, cmp func(int, int) int) iter.Seq2[int, int] {
		return Merge2Func(cmp, count2(n/2), squares2(n/2))
	})
}

func BenchmarkMerge2Ten(b *testing.B) {
	benchmark2(b, func(n int, cmp func(int, int) int) iter.Seq2[int, int] {
		return Merge2Func(cmp,
			sequence2(0, 1*n/10, 1),
			sequence2(1, 2*n/10, 2),
			sequence2(2, 3*n/10, 3),
			sequence2(3, 4*n/10, 4),
			sequence2(4, 5*n/10, 5),
			sequence2(5, 6*n/10, 6),
			sequence2(6, 7*n/10, 7),
			sequence2(7, 8*n/10, 8),
			sequence2(8, 9*n/10, 9),
			sequence2(9, 10*n/10, 10),
		)
	})
}

func benchmark2[K cmp.Ordered, V any](b *testing.B, merge func(int, func(K, K) int) iter.Seq2[K, V]) {
	comparisons := 0
	compare := func(a, b K) int {
		comparisons++
		return cmp.Compare(a, b)
	}
	start := time.Now()
	count := b.N
	for _, _ = range merge(count, compare) {
		if count--; count == 0 {
			break
		}
	}
	duration := time.Since(start)
	b.ReportMetric(float64(b.N)/duration.Seconds(), "merge/s")
	b.ReportMetric(float64(comparisons)/float64(b.N), "comp/op")
}
