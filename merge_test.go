package kway

import (
	"cmp"
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

func TestMerge(t *testing.T) {
	m := Merge(count(1), count(2), count(3))

	var values []int
	for v := range m {
		values = append(values, v)
	}

	if len(values) != 6 {
		t.Errorf("expected 6 values, got %d", len(values))
	}
	if !slices.Equal(values, []int{0, 0, 0, 1, 1, 2}) {
		t.Errorf("expected [0, 0, 0, 1, 1, 2], got %v", values)
	}
}

func BenchmarkMergeOne(b *testing.B) {
	benchmark(b, func(cmp func(int, int) int) iter.Seq[int] {
		return MergeFunc(cmp, count(1000))
	})
}

func BenchmarkMergeTwo(b *testing.B) {
	benchmark(b, func(cmp func(int, int) int) iter.Seq[int] {
		return MergeFunc(cmp, count(1000), squares(100))
	})
}

func BenchmarkMergeTen(b *testing.B) {
	benchmark(b, func(cmp func(int, int) int) iter.Seq[int] {
		return MergeFunc(cmp,
			sequence(0, 1000, 1),
			sequence(1, 2000, 2),
			sequence(2, 3000, 3),
			sequence(3, 4000, 4),
			sequence(4, 5000, 5),
			sequence(5, 6000, 6),
			sequence(6, 7000, 7),
			sequence(7, 8000, 8),
			sequence(8, 9000, 9),
			sequence(9, 10000, 10),
		)
	})
}

func benchmark[V cmp.Ordered](b *testing.B, merge func(func(V, V) int) iter.Seq[V]) {
	comparisons := 0
	compare := func(a, b V) int {
		comparisons++
		return cmp.Compare(a, b)
	}
	start := time.Now()
	count := 0
	for i := 0; i < b.N; i++ {
		count = 0
		for _ = range merge(compare) {
			count++
		}
	}
	duration := time.Since(start)
	b.ReportMetric(float64(duration)/float64(count*b.N), "ns/op")
	b.ReportMetric(float64(count*b.N)/duration.Seconds(), "merge/s")
	b.ReportMetric(float64(comparisons)/float64(count*b.N), "comp/op")
}

func BenchmarkMerge2One(b *testing.B) {
	benchmark2(b, func(cmp func(int, int) int) iter.Seq2[int, int] {
		return Merge2Func(cmp, count2(1000))
	})
}

func BenchmarkMerge2Two(b *testing.B) {
	benchmark2(b, func(cmp func(int, int) int) iter.Seq2[int, int] {
		return Merge2Func(cmp, count2(1000), squares2(100))
	})
}

func BenchmarkMerge2Ten(b *testing.B) {
	benchmark2(b, func(cmp func(int, int) int) iter.Seq2[int, int] {
		return Merge2Func(cmp,
			sequence2(0, 1000, 1),
			sequence2(1, 2000, 2),
			sequence2(2, 3000, 3),
			sequence2(3, 4000, 4),
			sequence2(4, 5000, 5),
			sequence2(5, 6000, 6),
			sequence2(6, 7000, 7),
			sequence2(7, 8000, 8),
			sequence2(8, 9000, 9),
			sequence2(9, 10000, 10),
		)
	})
}

func benchmark2[K cmp.Ordered, V any](b *testing.B, merge func(func(K, K) int) iter.Seq2[K, V]) {
	comparisons := 0
	compare := func(a, b K) int {
		comparisons++
		return cmp.Compare(a, b)
	}
	start := time.Now()
	count := 0
	for i := 0; i < b.N; i++ {
		for _, _ = range merge(compare) {
			count++
		}
	}
	duration := time.Since(start)
	b.ReportMetric(float64(duration)/float64(count), "ns/op")
	b.ReportMetric(float64(count)/duration.Seconds(), "merge/s")
	b.ReportMetric(float64(comparisons)/float64(count), "comp/op")
}
