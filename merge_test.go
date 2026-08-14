package kway

import (
	"bytes"
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

// TestMergeBufferBoundaries covers empty inputs and each point where an
// input buffer would otherwise grow after yielding a full batch.
func TestMergeBufferBoundaries(t *testing.T) {
	const fanIn = 8
	lengths := [...]int{0, minBufferSize, 3 * minBufferSize, 7 * minBufferSize, 15 * minBufferSize}

	for _, n := range lengths {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			seqs := make([]iter.Seq2[int, error], fanIn)
			for i := range seqs {
				seqs[i] = sequence(i, fanIn*n+i, fanIn)
			}

			got, err := values(MergeFunc(cmp.Compare[int], seqs...))
			if err != nil {
				t.Fatal(err)
			}
			want := make([]int, fanIn*n)
			for i := range want {
				want[i] = i
			}
			if !slices.Equal(got, want) {
				t.Errorf("expected %v, got %v", want, got)
			}
		})
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

func sliceSeq[T any](batches [][]T) iter.Seq2[[]T, error] {
	return func(yield func([]T, error) bool) {
		for _, b := range batches {
			if !yield(b, nil) {
				return
			}
		}
	}
}

func alternatingRuns(runCount, runLength int) (values0, values1 []int) {
	values0 = make([]int, 0, runCount*runLength)
	values1 = make([]int, 0, runCount*runLength)
	for run := 0; run < 2*runCount; run++ {
		for value := run * runLength; value < (run+1)*runLength; value++ {
			if run%2 == 0 {
				values0 = append(values0, value)
			} else {
				values1 = append(values1, value)
			}
		}
	}
	return values0, values1
}

func formatValues(values []int) [][]byte {
	formatted := make([][]byte, len(values))
	for i, value := range values {
		formatted[i] = []byte(formatValue(value))
	}
	return formatted
}

type taggedValue struct {
	key    int
	source int
}

func taggedValues(source int, keys ...int) []taggedValue {
	values := make([]taggedValue, len(keys))
	for i, key := range keys {
		values[i] = taggedValue{key: key, source: source}
	}
	return values
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

// TestMergeSliceAlternatingRunComparisons checks both winner directions, the
// non-galloping path, failed lookaheads, and gallop boundaries. Its comparison
// counts match a simple two-way merge whenever a cacheable boundary exists.
func TestMergeSliceAlternatingRunComparisons(t *testing.T) {
	const runCount = 1000

	for _, test := range []struct {
		runLength       int
		wantComparisons int
	}{
		{runLength: 1, wantComparisons: 1999},
		{runLength: 2, wantComparisons: 3998},
		{runLength: 4, wantComparisons: 7996},
		{runLength: 8, wantComparisons: 15991},
		{runLength: 16, wantComparisons: 19990},
		{runLength: 32, wantComparisons: 23988},
		{runLength: 64, wantComparisons: 27986},
	} {
		for _, reversed := range []bool{false, true} {
			t.Run(fmt.Sprintf("run=%d/reversed=%t", test.runLength, reversed), func(t *testing.T) {
				values0, values1 := alternatingRuns(runCount, test.runLength)
				if reversed {
					values0, values1 = values1, values0
				}
				comparisons := 0
				compare := func(a, b int) int {
					comparisons++
					return cmp.Compare(a, b)
				}

				got, err := concatValues(MergeSliceFunc(compare,
					sliceSeq([][]int{values0}),
					sliceSeq([][]int{values1}),
				))
				if err != nil {
					t.Fatal(err)
				}
				want := make([]int, len(values0)+len(values1))
				for i := range want {
					want[i] = i
				}
				if !slices.Equal(got, want) {
					t.Fatal("merged values are not ordered")
				}
				if comparisons != test.wantComparisons {
					t.Fatalf("expected %d comparisons, got %d", test.wantComparisons, comparisons)
				}
			})
		}
	}
}

// TestMergeSliceCachedComparisons checks that cached equality results retain
// the merge's first-input tie order, and that a cache does not cross batches.
func TestMergeSliceCachedComparisons(t *testing.T) {
	tests := []struct {
		name             string
		values0, values1 [][]taggedValue
		want             []taggedValue
		comparisons      int
	}{
		{
			name:    "first lookahead tie",
			values0: [][]taggedValue{taggedValues(0, 0, 1, 2, 4)},
			values1: [][]taggedValue{taggedValues(1, 2, 3, 5)},
			want: []taggedValue{
				{key: 0, source: 0}, {key: 1, source: 0},
				{key: 2, source: 0}, {key: 2, source: 1},
				{key: 3, source: 1}, {key: 4, source: 0}, {key: 5, source: 1},
			},
			comparisons: 5,
		},
		{
			name:    "second lookahead tie",
			values0: [][]taggedValue{taggedValues(0, 2, 3, 5)},
			values1: [][]taggedValue{taggedValues(1, 0, 1, 2, 4)},
			want: []taggedValue{
				{key: 0, source: 1}, {key: 1, source: 1},
				{key: 2, source: 0}, {key: 2, source: 1},
				{key: 3, source: 0}, {key: 4, source: 1}, {key: 5, source: 0},
			},
			comparisons: 5,
		},
		{
			name:    "first gallop tie",
			values0: [][]taggedValue{taggedValues(0, 0, 1, 2, 3, 4)},
			values1: [][]taggedValue{taggedValues(1, 3, 5)},
			want: []taggedValue{
				{key: 0, source: 0}, {key: 1, source: 0}, {key: 2, source: 0},
				{key: 3, source: 0}, {key: 3, source: 1},
				{key: 4, source: 0}, {key: 5, source: 1},
			},
			comparisons: 5,
		},
		{
			name:    "second gallop tie",
			values0: [][]taggedValue{taggedValues(0, 3, 5)},
			values1: [][]taggedValue{taggedValues(1, 0, 1, 2, 3, 4)},
			want: []taggedValue{
				{key: 0, source: 1}, {key: 1, source: 1}, {key: 2, source: 1},
				{key: 3, source: 0}, {key: 3, source: 1},
				{key: 4, source: 1}, {key: 5, source: 0},
			},
			comparisons: 5,
		},
		{
			name: "batch boundary",
			values0: [][]taggedValue{
				taggedValues(0, 0, 1),
				taggedValues(0, 2, 4),
			},
			values1: [][]taggedValue{
				taggedValues(1, 2, 3),
				taggedValues(1, 5),
			},
			want: []taggedValue{
				{key: 0, source: 0}, {key: 1, source: 0},
				{key: 2, source: 0}, {key: 2, source: 1},
				{key: 3, source: 1}, {key: 4, source: 0}, {key: 5, source: 1},
			},
			comparisons: 5,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			comparisons := 0
			compare := func(a, b taggedValue) int {
				comparisons++
				return cmp.Compare(a.key, b.key)
			}

			got, err := concatValues(MergeSliceFunc(compare,
				sliceSeq(test.values0),
				sliceSeq(test.values1),
			))
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, test.want) {
				t.Errorf("expected %v, got %v", test.want, got)
			}
			if comparisons != test.comparisons {
				t.Errorf("expected %d comparisons, got %d", test.comparisons, comparisons)
			}
		})
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

// recycledSource produces the values start, start+step, start+2*step, ... as
// zero-padded numbers, in batches of at most batchSize elements of T = []byte
// which all point into a single buffer that the source overwrites on every
// pull. It emulates sources like parquet-go's row readers, which recycle the
// memory holding the values they yielded once they are asked for more.
func recycledSource(start, step, count, batchSize int) iter.Seq2[[][]byte, error] {
	return func(yield func([][]byte, error) bool) {
		buf := make([]byte, batchSize*valueWidth)
		batch := make([][]byte, batchSize)

		for i := 0; i < count; i += batchSize {
			for j := range buf {
				buf[j] = '#' // poison the values yielded by the previous pull
			}
			n := min(batchSize, count-i)
			for j := range n {
				value := buf[j*valueWidth : (j+1)*valueWidth]
				copy(value, formatValue(start+(i+j)*step))
				batch[j] = value
			}
			if !yield(batch[:n], nil) {
				return
			}
		}
	}
}

const valueWidth = 8

func formatValue(i int) string {
	return fmt.Sprintf("%0*d", valueWidth, i)
}

// TestMergeSliceRecycledBuffers verifies that the merge does not read values
// from a batch after pulling the next one from the sequence that produced it:
// the values are backed by memory that the sequences recycle across pulls, so
// any value copied into the output buffer but not yielded yet before a refill
// would be corrupted.
func TestMergeSliceRecycledBuffers(t *testing.T) {
	const count = 1000

	// Small batches interleave the refills with the merge; batches larger than
	// minBufferSize also exercise the paths passing the batches through to the
	// caller without copying them.
	for _, k := range []int{2, 3, 8} {
		for _, batchSize := range []int{1, 5, 32, 128} {
			for _, duplicate := range []bool{false, true} {
				name := fmt.Sprintf("k=%d,batch=%d,duplicate=%t", k, batchSize, duplicate)

				t.Run(name, func(t *testing.T) {
					seqs := make([]iter.Seq2[[][]byte, error], k)
					var want []string

					if duplicate {
						// All the sequences produce the same values.
						for i := range seqs {
							seqs[i] = recycledSource(0, 1, count, batchSize)
						}
						for i := range count {
							for range k {
								want = append(want, formatValue(i))
							}
						}
					} else {
						// Sequence i produces the values i, i+k, i+2k, ...
						for i := range seqs {
							n := (count - i + k - 1) / k
							seqs[i] = recycledSource(i, k, n, batchSize)
						}
						for i := range count {
							want = append(want, formatValue(i))
						}
					}

					got := make([]string, 0, len(want))
					for values, err := range MergeSliceFunc(bytes.Compare, seqs...) {
						if err != nil {
							t.Fatal(err)
						}
						for _, value := range values {
							got = append(got, string(value))
						}
					}

					if !slices.Equal(got, want) {
						t.Errorf("expected %q, got %q", want, got)
					}
				})
			}
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

//go:noinline
func finiteBlocksSlice(start, size, n int) iter.Seq2[[]int, error] {
	return func(yield func([]int, error) bool) {
		values := make([]int, size)
		for b := range n {
			base := start + b*size
			for j := range values {
				values[j] = base + j
			}
			if !yield(values, nil) {
				return
			}
		}
	}
}

//go:noinline
func interleavedSlice(i, k, size, n int) iter.Seq2[[]int, error] {
	return func(yield func([]int, error) bool) {
		values := make([]int, size)
		for b := range n {
			base := b * k * size
			for j := range values {
				values[j] = base + i + j*k
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

// BenchmarkMergeFirstValue8 measures the time from creating long input
// sequences to receiving the first merged value.
func BenchmarkMergeFirstValue8(b *testing.B) {
	const fanIn = 8

	for range b.N {
		seqs := make([]iter.Seq2[int, error], fanIn)
		for i := range seqs {
			seqs[i] = sequence(i, 1<<62, fanIn)
		}

		got := 0
		ok := false
		for value, err := range MergeFunc(cmp.Compare[int], seqs...) {
			if err != nil {
				b.Fatal(err)
			}
			got = value
			ok = true
			break
		}
		if !ok || got != 0 {
			b.Fatalf("expected first value 0, got %d", got)
		}
	}
}

// BenchmarkMergeBufferLong8 measures throughput after each input buffer has
// grown to its maximum size.
func BenchmarkMergeBufferLong8(b *testing.B) {
	const (
		fanIn  = 8
		length = 8 * bufferSize
	)

	b.ReportAllocs()
	for range b.N {
		seqs := make([]iter.Seq2[int, error], fanIn)
		for i := range seqs {
			seqs[i] = sequence(i, fanIn*length+i, fanIn)
		}

		got := 0
		for _, err := range MergeFunc(cmp.Compare[int], seqs...) {
			if err != nil {
				b.Fatal(err)
			}
			got++
		}
		if got != fanIn*length {
			b.Fatalf("expected %d values, got %d", fanIn*length, got)
		}
	}
}

func BenchmarkMergeBufferBoundaries8(b *testing.B) {
	const fanIn = 8
	lengths := [...]int{0, minBufferSize, 3 * minBufferSize, 7 * minBufferSize, 15 * minBufferSize}

	b.ReportAllocs()
	for range b.N {
		for _, n := range lengths {
			seqs := make([]iter.Seq2[int, error], fanIn)
			for i := range seqs {
				seqs[i] = sequence(i, fanIn*n+i, fanIn)
			}

			got := 0
			for _, err := range MergeFunc(cmp.Compare[int], seqs...) {
				if err != nil {
					b.Fatal(err)
				}
				got++
			}
			if got != fanIn*n {
				b.Fatalf("expected %d values, got %d", fanIn*n, got)
			}
		}
	}
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

// BenchmarkMergeSliceZeroCopyBatches exercises five consecutive zero-copy
// output batches from three sources. After the first aggregated batch, the
// next five batches from the first source sort before its challenger.
func BenchmarkMergeSliceZeroCopyBatches(b *testing.B) {
	seqs := []iter.Seq2[[]int, error]{
		finiteBlocksSlice(0, minBufferSize, 6),
		finiteBlocksSlice(1<<20, minBufferSize, 1),
		finiteBlocksSlice(2<<20, minBufferSize, 1),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values := 0
		batches := 0
		for batch, err := range MergeSliceFunc(cmp.Compare[int], seqs...) {
			if err != nil {
				b.Fatal(err)
			}
			values += len(batch)
			if batches++; batches == 6 {
				break
			}
		}
		if values != 6*minBufferSize || batches != 6 {
			b.Fatalf("expected six batches containing %d values, got %d batches containing %d values", 6*minBufferSize, batches, values)
		}
	}
}

func BenchmarkMergeSliceInterleaved3(b *testing.B) {
	const batches = 8

	seqs := []iter.Seq2[[]int, error]{
		interleavedSlice(0, 3, bufferSize, batches),
		interleavedSlice(1, 3, bufferSize, batches),
		interleavedSlice(2, 3, bufferSize, batches),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values := 0
		for batch, err := range MergeSliceFunc(cmp.Compare[int], seqs...) {
			if err != nil {
				b.Fatal(err)
			}
			values += len(batch)
		}
		if values != len(seqs)*batches*bufferSize {
			b.Fatalf("expected %d values, got %d", len(seqs)*batches*bufferSize, values)
		}
	}
}

// BenchmarkMergeSliceAlternatingRuns measures the end-to-end latency of
// merging two one-batch inputs whose runs alternate from the non-galloping
// path through the gallop crossover. The inputs use fixed-width byte keys and
// bytes.Compare.
func BenchmarkMergeSliceAlternatingRuns(b *testing.B) {
	benchmarkMergeSliceAlternatingRuns(b, false)
}

// BenchmarkMergeSliceComparatorCalls reports comparator calls separately, so
// incrementing the counter does not affect the latency benchmark.
func BenchmarkMergeSliceComparatorCalls(b *testing.B) {
	benchmarkMergeSliceAlternatingRuns(b, true)
}

func benchmarkMergeSliceAlternatingRuns(b *testing.B, countComparisons bool) {
	const runCount = 1000

	for _, runLength := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("run=%d", runLength), func(b *testing.B) {
			values0, values1 := alternatingRuns(runCount, runLength)
			seq0 := sliceSeq([][][]byte{formatValues(values0)})
			seq1 := sliceSeq([][][]byte{formatValues(values1)})
			valueCount := len(values0) + len(values1)

			got, err := concatValues(MergeSliceFunc(bytes.Compare, seq0, seq1))
			if err != nil {
				b.Fatal(err)
			}
			if len(got) != valueCount || !slices.IsSortedFunc(got, bytes.Compare) {
				b.Fatalf("expected %d ordered values, got %d", valueCount, len(got))
			}

			comparisons := 0
			compare := bytes.Compare
			if countComparisons {
				compare = func(a, b []byte) int {
					comparisons++
					return bytes.Compare(a, b)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			values := 0
			for i := 0; i < b.N; i++ {
				for batch, err := range MergeSliceFunc(compare, seq0, seq1) {
					if err != nil {
						b.Fatal(err)
					}
					values += len(batch)
				}
			}
			b.StopTimer()

			if values != b.N*valueCount {
				b.Fatalf("expected %d values, got %d", b.N*valueCount, values)
			}
			if countComparisons {
				b.ReportMetric(float64(comparisons)/float64(b.N), "comp/op")
			}
		})
	}
}
