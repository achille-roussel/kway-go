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

func repeat(n int, words []string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for i := range n {
			if !yield(words[i%len(words)]) {
				return
			}
		}
	}
}

var testwords = []string{"foo", "bar", "baz", "qux", "quux", "corge", "grault", "garply", "waldo", "fred"}

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
	benchmark(b, func() iter.Seq[int] {
		return Merge(count(1000))
	})
}

func BenchmarkMergeTwo(b *testing.B) {
	benchmark(b, func() iter.Seq[int] {
		return Merge(count(1000), squares(100))
	})
}

func BenchmarkMergeTen(b *testing.B) {
	benchmark(b, func() iter.Seq[string] {
		return Merge(
			repeat(1000, testwords[0:]),
			repeat(1000, testwords[1:]),
			repeat(1000, testwords[2:]),
			repeat(1000, testwords[3:]),
			repeat(1000, testwords[4:]),
			repeat(1000, testwords[5:]),
			repeat(1000, testwords[6:]),
			repeat(1000, testwords[7:]),
			repeat(1000, testwords[8:]),
			repeat(1000, testwords[9:]),
		)
	})
}

func benchmark[V cmp.Ordered](b *testing.B, merge func() iter.Seq[V]) {
	start := time.Now()
	count := 0
	for i := 0; i < b.N; i++ {
		count = 0
		for _ = range merge() {
			count++
		}
	}
	duration := time.Since(start)
	b.ReportMetric(float64(duration)/float64(count*b.N), "ns/op")
	b.ReportMetric(float64(count*b.N)/duration.Seconds(), "merge/s")
}

func BenchmarkMerge2One(b *testing.B) {
	benchmark2(b, func() iter.Seq2[int, int] {
		return Merge2(count2(1000))
	})
}

func BenchmarkMerge2Two(b *testing.B) {
	benchmark2(b, func() iter.Seq2[int, int] {
		return Merge2(count2(1000), squares2(100))
	})
}

func BenchmarkMerge2Ten(b *testing.B) {
	benchmark2(b, func() iter.Seq2[int, int] {
		return Merge2(
			count2(1000),
			count2(1000),
			count2(1000),
			count2(1000),
			count2(1000),
			squares2(100),
			squares2(100),
			squares2(100),
			squares2(100),
			squares2(100),
		)
	})
}

func benchmark2[K cmp.Ordered, V any](b *testing.B, merge func() iter.Seq2[K, V]) {
	start := time.Now()
	count := 0
	for i := 0; i < b.N; i++ {
		count = 0
		for _, _ = range merge() {
			count++
		}
	}
	duration := time.Since(start)
	b.ReportMetric(float64(duration)/float64(count*b.N), "ns/op")
	b.ReportMetric(float64(count*b.N)/duration.Seconds(), "merge/s")
}
