package kway

import (
	"cmp"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
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

func intSeq(values []int) iter.Seq2[int, error] {
	return func(yield func(int, error) bool) {
		for _, v := range values {
			if !yield(v, nil) {
				return
			}
		}
	}
}

func sliceSeq(batches [][]int) iter.Seq2[[]int, error] {
	return func(yield func([]int, error) bool) {
		for _, b := range batches {
			if !yield(b, nil) {
				return
			}
		}
	}
}

// TestMergeBlocks exercises run-structured inputs: each sequence produces
// interleaved blocks of consecutive values, triggering the bulk-copy and
// zero-copy passthrough paths of the merge algorithms.
func TestMergeBlocks(t *testing.T) {
	for _, k := range []int{2, 3, 5, 8} {
		for _, size := range []int{1, 3, 32, 200} {
			t.Run(fmt.Sprintf("k=%d,size=%d", k, size), func(t *testing.T) {
				const numBlocks = 5
				data := make([][]int, k)
				var want []int
				for i := range data {
					for b := 0; b < numBlocks; b++ {
						base := (b*k + i) * size
						for j := 0; j < size; j++ {
							data[i] = append(data[i], base+j)
						}
					}
					want = append(want, data[i]...)
				}
				slices.Sort(want)

				seqs := make([]iter.Seq2[int, error], k)
				for i := range seqs {
					seqs[i] = intSeq(data[i])
				}
				got, err := values(Merge(seqs...))
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(got, want) {
					t.Errorf("Merge: expected %v, got %v", want, got)
				}

				sseqs := make([]iter.Seq2[[]int, error], k)
				for i := range sseqs {
					var batches [][]int
					for v := data[i]; len(v) > 0; {
						n := min(size, len(v))
						batches = append(batches, v[:n])
						v = v[n:]
					}
					sseqs[i] = sliceSeq(batches)
				}
				got, err = concatValues(MergeSlice(sseqs...))
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(got, want) {
					t.Errorf("MergeSlice: expected %v, got %v", want, got)
				}
			})
		}
	}
}

// TestMergeRandom validates the merge algorithms against a sort-based
// reference on randomized inputs: random sequence counts, lengths, value
// distributions, and batch partitions (including empty batches).
func TestMergeRandom(t *testing.T) {
	prng := rand.New(rand.NewPCG(0, 1))

	for trial := 0; trial < 200; trial++ {
		k := 1 + prng.IntN(9)
		limit := []int{10, 100, 100000}[trial%3]
		data := make([][]int, k)
		var want []int
		for i := range data {
			vs := make([]int, prng.IntN(500))
			for j := range vs {
				vs[j] = prng.IntN(limit)
			}
			slices.Sort(vs)
			data[i] = vs
			want = append(want, vs...)
		}
		slices.Sort(want)

		seqs := make([]iter.Seq2[int, error], k)
		for i := range seqs {
			seqs[i] = intSeq(data[i])
		}
		got, err := values(Merge(seqs...))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("trial %d: Merge of %d sequences produced wrong values", trial, k)
		}

		sseqs := make([]iter.Seq2[[]int, error], k)
		for i := range sseqs {
			var batches [][]int
			for v := data[i]; len(v) > 0; {
				if prng.IntN(10) == 0 {
					batches = append(batches, nil)
				}
				n := min(1+prng.IntN(200), len(v))
				batches = append(batches, v[:n])
				v = v[n:]
			}
			sseqs[i] = sliceSeq(batches)
		}
		got, err = concatValues(MergeSlice(sseqs...))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("trial %d: MergeSlice of %d sequences produced wrong values", trial, k)
		}
	}
}

// TestMergeErrorBetweenRuns injects an error in the middle of a sequence of
// run-structured inputs, exercising error handling in the bulk-copy and
// zero-copy passthrough paths.
func TestMergeErrorBetweenRuns(t *testing.T) {
	errval := errors.New("test")

	for _, k := range []int{2, 3, 5} {
		t.Run(fmt.Sprint(k), func(t *testing.T) {
			const runLen = 500
			seqs := make([]iter.Seq2[int, error], k)
			var want []int
			for i := range seqs {
				vs := make([]int, runLen)
				for j := range vs {
					vs[j] = i*runLen + j
				}
				want = append(want, vs...)
				if i == 1 {
					seqs[i] = func(yield func(int, error) bool) {
						for j, v := range vs {
							if j == runLen/2 && !yield(0, errval) {
								return
							}
							if !yield(v, nil) {
								return
							}
						}
					}
				} else {
					seqs[i] = intSeq(vs)
				}
			}
			slices.Sort(want)

			var got []int
			errCount := 0
			for v, err := range Merge(seqs...) {
				if err != nil {
					if err != errval {
						t.Fatal(err)
					}
					errCount++
				} else {
					got = append(got, v)
				}
			}
			if errCount != 1 {
				t.Errorf("expected 1 error, got %d", errCount)
			}
			if !slices.Equal(got, want) {
				t.Errorf("expected %d values in order, got %d", len(want), len(got))
			}
		})
	}
}

// TestMergeStopEarly stops consuming the merged sequence at various points,
// in particular during the passthrough phase after other sequences have been
// exhausted, which must not call yield again after it returned false.
func TestMergeStopEarly(t *testing.T) {
	for _, k := range []int{2, 3} {
		for _, stop := range []int{1, 10, 100, 500} {
			t.Run(fmt.Sprintf("k=%d,stop=%d", k, stop), func(t *testing.T) {
				seqs := make([]iter.Seq2[int, error], k)
				for i := range seqs {
					n := 3
					if i == k-1 {
						n = 1000
					}
					vs := make([]int, n)
					for j := range vs {
						vs[j] = i + j*k
					}
					seqs[i] = intSeq(vs)
				}
				n := 0
				for _, err := range Merge(seqs...) {
					if err != nil {
						t.Fatal(err)
					}
					if n++; n == stop {
						break
					}
				}
				if n != stop {
					t.Errorf("expected to stop after %d values, got %d", stop, n)
				}
			})
		}
	}
}

//go:noinline
func blocks(i, k, size int) iter.Seq2[int, error] {
	return func(yield func(int, error) bool) {
		for b := 0; ; b++ {
			base := (b*k + i) * size
			for j := 0; j < size; j++ {
				if !yield(base+j, nil) {
					return
				}
			}
		}
	}
}

//go:noinline
func blocksSlice(i, k, size int) iter.Seq2[[]int, error] {
	return func(yield func([]int, error) bool) {
		values := make([]int, size)
		for b := 0; ; b++ {
			base := (b*k + i) * size
			for j := range values {
				values[j] = base + j
			}
			if !yield(values, nil) {
				return
			}
		}
	}
}

func BenchmarkMergeBlocks2(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq2[int, error] {
		return MergeFunc(cmp,
			blocks(0, 2, 32),
			blocks(1, 2, 32),
		)
	})
}

func BenchmarkMergeBlocks3(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq2[int, error] {
		return MergeFunc(cmp,
			blocks(0, 3, 32),
			blocks(1, 3, 32),
			blocks(2, 3, 32),
		)
	})
}

func BenchmarkMergeBlocks8(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq2[int, error] {
		seqs := make([]iter.Seq2[int, error], 8)
		for i := range seqs {
			seqs[i] = blocks(i, 8, 32)
		}
		return MergeFunc(cmp, seqs...)
	})
}

func BenchmarkMergeInterleaved8(b *testing.B) {
	benchmark(b, func(n int, cmp func(int, int) int) iter.Seq2[int, error] {
		seqs := make([]iter.Seq2[int, error], 8)
		for i := range seqs {
			seqs[i] = sequence(i, 1<<62, 8)
		}
		return MergeFunc(cmp, seqs...)
	})
}

func BenchmarkMergeSliceBlocks2(b *testing.B) {
	benchmarkSlice(b, func(n int, cmp func(int, int) int) iter.Seq2[[]int, error] {
		return MergeSliceFunc(cmp,
			blocksSlice(0, 2, 128),
			blocksSlice(1, 2, 128),
		)
	})
}

func BenchmarkMergeSliceBlocks3(b *testing.B) {
	benchmarkSlice(b, func(n int, cmp func(int, int) int) iter.Seq2[[]int, error] {
		return MergeSliceFunc(cmp,
			blocksSlice(0, 3, 128),
			blocksSlice(1, 3, 128),
			blocksSlice(2, 3, 128),
		)
	})
}
