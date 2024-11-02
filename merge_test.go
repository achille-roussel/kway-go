package kway

import (
	"cmp"
	"errors"
	"fmt"
	"iter"
	"slices"
	"testing"
	"time"
)

//go:noinline
func countSlice(n, r int) iter.Seq2[[]int, error] {
	return func(yield func([]int, error) bool) {
		values := make([]int, r)
		for i := range n {
			n := i * r
			for j := range values {
				values[j] = n + j
			}
			if !yield(values, nil) {
				return
			}
		}
	}
}

//go:noinline
func count(n int) iter.Seq2[int, error] {
	return func(yield func(int, error) bool) {
		for i := range n {
			if !yield(i, nil) {
				return
			}
		}
	}
}

//go:noinline
func sequence(min, max, step int) iter.Seq2[int, error] {
	return func(yield func(int, error) bool) {
		for i := min; i < max; i += step {
			if !yield(i, nil) {
				return
			}
		}
	}
}

func TestMerge(t *testing.T) {
	for n := range 10 {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			seqs := make([]iter.Seq2[int, error], n)
			for i := range seqs {
				seqs[i] = count(i)
			}

			assertCorrectMerge(t, seqs)
		})
	}
}

func TestMerge2(t *testing.T) {
	it := func(s []int) iter.Seq2[int, error] {
		return func(yield func(int, error) bool) {
			for i := range s {
				if !yield(s[i], nil) {
					return
				}
			}
		}
	}
	cases := []struct {
		name string
		s1   []int
		s2   []int
	}{
		{
			name: "interleaved slices",
			s1:   []int{0, 3},
			s2:   []int{2, 5},
		},
		{
			name: "interleaved slices",
			s1:   []int{2, 5},
			s2:   []int{0, 3},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			seqs := []iter.Seq2[int, error]{it(c.s1), it(c.s2)}
			assertCorrectMerge(t, seqs)
		})
	}
}

func assertCorrectMerge(t *testing.T, seqs []iter.Seq2[int, error]) {
	want := make([]int, 0)
	for _, seq := range seqs {
		v, err := values(seq)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, v...)
	}
	slices.Sort(want)

	seq := Merge(seqs...)
	got, err := values(seq)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestMergeContinueAfterError2(t *testing.T) {
	errval := errors.New("")

	seq0 := func(yield func(int, error) bool) {
		for i := 0; i < 5; i++ {
			if !yield(i, nil) {
				return
			}
		}
		if !yield(0, errval) {
			return
		}
		for i := 5; i < 10; i++ {
			if !yield(i, nil) {
				return
			}
		}
	}

	seq1 := func(yield func(int, error) bool) {
		for i := 0; i < 10; i++ {
			if !yield(i, nil) {
				return
			}
		}
	}

	var values []int
	var hasError bool
	for v, err := range Merge(seq0, seq1) {
		if err != nil {
			if v != 0 {
				t.Errorf("expected 0, got %v", v)
			}
			if err != errval {
				t.Fatal(err)
			}
			hasError = true
		} else {
			values = append(values, v)
		}
	}

	expect := []int{
		0, 0, 1, 1, 2, 2, 3, 3, 4, 4,
		5, 5, 6, 6, 7, 7, 8, 8, 9, 9,
	}
	if !slices.Equal(values, expect) {
		t.Errorf("expected %v, got %v", expect, values)
	}
	if !hasError {
		t.Error("expected error")
	}
}

func TestMergeContinueAfterError3(t *testing.T) {
	errval := errors.New("")

	seq0 := func(yield func(int, error) bool) {
		for i := 0; i < 5; i++ {
			if !yield(i, nil) {
				return
			}
		}
		if !yield(0, errval) {
			return
		}
		for i := 5; i < 10; i++ {
			if !yield(i, nil) {
				return
			}
		}
	}

	seq1 := func(yield func(int, error) bool) {
		for i := 0; i < 10; i++ {
			if !yield(i, nil) {
				return
			}
		}
	}

	var values []int
	var errCount int
	for v, err := range Merge(seq0, seq1, seq0) {
		if err != nil {
			if v != 0 {
				t.Errorf("expected 0, got %v", v)
			}
			if err != errval {
				t.Fatal(err)
			}
			errCount++
		} else {
			values = append(values, v)
		}
	}

	expect := []int{
		0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4,
		5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9,
	}
	if !slices.Equal(values, expect) {
		t.Errorf("expected %v, got %v", expect, values)
	}
	if errCount != 2 {
		t.Error("expected error")
	}
}

func values[T any](seq iter.Seq2[T, error]) (values []T, err error) {
	for v, err := range seq {
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

func BenchmarkMerge1(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq2[int, error] {
		return MergeFunc(cmp, count(n))
	})
}

func BenchmarkMerge2(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq2[int, error] {
		return MergeFunc(cmp,
			sequence(0, n-(n/4), 1),
			sequence(n/4, n, 2),
		)
	})
}

func BenchmarkMerge3(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq2[int, error] {
		return MergeFunc(cmp,
			sequence(0, n, 2),
			sequence(n/4, n, 1),
			sequence(n/3, n, 3),
		)
	})
}

func benchmark[V cmp.Ordered](b *testing.B, merge func(int, func(V, V) int) iter.Seq2[V, error]) {
	comparisons := 0
	compare := func(a, b V) int {
		comparisons++
		return cmp.Compare(a, b)
	}
	start := time.Now()
	count := b.N
	for _, err := range merge(count, compare) {
		if err != nil {
			b.Fatal(err)
		}
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

func TestMergeSlice(t *testing.T) {
	for n := range 10 {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			seqs := make([]iter.Seq2[[]int, error], n)
			want := make([]int, 0, 2*n)

			for i := range seqs {
				seqs[i] = countSlice(i, 10)
				v, err := values(count(i * 10))
				if err != nil {
					t.Fatal(err)
				}
				want = append(want, v...)
			}

			slices.Sort(want)
			seq := MergeSlice(seqs...)

			got, err := concatValues(seq)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want) {
				t.Errorf("expected %v, got %v", want, got)
			}
		})
	}
}

func concatValues[T any](seq iter.Seq2[[]T, error]) (values []T, err error) {
	for v, err := range seq {
		if err != nil {
			return nil, err
		}
		values = append(values, v...)
	}
	return values, nil
}

func BenchmarkMergeSlice1(b *testing.B) {
	benchmarkSlice(b, func(n int, cmp func(int, int) int) iter.Seq2[[]int, error] {
		return MergeSliceFunc(cmp, countSlice(n, 100))
	})
}

func BenchmarkMergeSlice2(b *testing.B) {
	benchmarkSlice(b, func(n int, cmp func(int, int) int) iter.Seq2[[]int, error] {
		return MergeSliceFunc(cmp,
			countSlice(n, 100),
			countSlice(n, 127),
		)
	})
}

func BenchmarkMergeSlice3(b *testing.B) {
	benchmarkSlice(b, func(n int, cmp func(int, int) int) iter.Seq2[[]int, error] {
		return MergeSliceFunc(cmp,
			countSlice(n, 100),
			countSlice(n, 101),
			countSlice(n, 127),
		)
	})
}

func benchmarkSlice[V cmp.Ordered](b *testing.B, merge func(int, func(V, V) int) iter.Seq2[[]V, error]) {
	comparisons := 0
	compare := func(a, b V) int {
		comparisons++
		return cmp.Compare(a, b)
	}
	start := time.Now()
	count := b.N
	for values, err := range merge(count, compare) {
		if err != nil {
			b.Fatal(err)
		}
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
