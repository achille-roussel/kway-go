// Package kway implements k-way merge algorithms for range functions.
package kway

import (
	"cmp"
	"iter"
)

const (
	// bufferSize is the size of the buffer used to read values from the
	// sequences.
	//
	// Note: I would like to avoid making this configurable, but I am also
	// aware that in latency-sensitive applications, it might be preferable
	// to have a smaller buffer size (or none at all), so values are produced
	// as soon as they are available. I would like to delay this change until
	// there is production data available to prove that it is needed, in my
	// experience, k-way merges tend to be used in batch processing systems
	// where throughput matters more than latency. One approach I would like
	// to experiment with is exponentially growing the buffer size (up to a
	// limit), so the merge algorithm can start with a small buffer size which
	// allows the first few values to be produces immediately, and then grow
	// to optimize for high throughput use cases.
	bufferSize = 128
)

// Merge merges multiple sequences into one. The sequences must produce ordered
// values. The algorithm complexity is O(n log k), where n is the total number
// of values to merge, and k is the number of sequences.
//
// The implementation is based on a loser-tree data structure, which minimizes
// the number of calls to the comparison function compared to the typical use
// of a min-heap.
//
// The function returns a sequence that yields merged values and is intended to
// be used in a for-range loop:
//
//	for v, err := range kway.Merge(seq0, seq1, seq2) {
//		if err != nil {
//			...
//		}
//		...
//	}
//
// The algorithm is implemented for sequences of pairs that produce either a
// value or a non-nil error. This design decision was made because k-way merges
// are most often used in distributed streaming systems where each sequence may
// be read from a remote source, and errors could occur when reading the values.
// For use cases where the sequences cannot produce errors, the conversion is
// straightforward:
//
//	func noerr[T any](seq iter.Seq[T]) iter.Seq2[T, error] {
//		return func(yield func(T, error) bool) {
//			for value := range seq {
//				if !yield(value, nil) {
//					return
//				}
//			}
//		}
//	}
//
// The inner implementation of the merge algorithm does not spawn goroutines to
// concurrently read values from the sequences. In some cases where values are
// retrieved from remote sources, it can become a performance bottleneck because
// the total time for the merge becomes bound on the sum of read latency.
// In those cases, it is recommended to wrap the sequences so values can be
// retrieved concurrently from the remote sources and psuhed into the merge
// algorithm via a channel.
//
// For applications that aim to achieve the highest throughput should also use
// MergeSlice instead, as it allows end-to-end batching which greatly amortizes
// the baseline cost of coroutine context switch in the Go runtime.
//
// See MergeFunc for a version of this function that allows the caller to pass
// a custom comparison function.
func Merge[T cmp.Ordered](seqs ...iter.Seq2[T, error]) iter.Seq2[T, error] {
	return MergeFunc(cmp.Compare[T], seqs...)
}

// MergeFunc merges multiple sequences into one using the given comparison
// function to determine the order of values. The sequences must be ordered
// by the same comparison function.
//
// See Merge for more details.
func MergeFunc[T any](cmp func(T, T) int, seqs ...iter.Seq2[T, error]) iter.Seq2[T, error] {
	if len(seqs) == 1 {
		return seqs[0]
	}
	var merged iter.Seq2[[]T, error]
	if len(seqs) == 2 {
		seq0 := buffer(bufferSize, seqs[0])
		seq1 := buffer(bufferSize, seqs[1])
		merged = merge2(cmp, seq0, seq1)
	} else {
		bufferedSeqs := make([]iter.Seq2[[]T, error], len(seqs))
		for i, seq := range seqs {
			bufferedSeqs[i] = buffer(bufferSize, seq)
		}
		merged = merge(cmp, bufferedSeqs)
	}
	return unbuffer(merged)
}

// MergeSlice merges multiple sequences producing slices of ordered values.
//
// The function is intended to be used in applications that have high-throughput
// requirements. By merging slices instead of individual values, the function
// amortizes the baseline costs such as time spent on coroutine context switch
// in the Go runtime, error checks, etc...
//
// The slices yielded when ranging over the returned function may or may not be
// slices that were produced by the input sequences. The function may choose to
// apply buffering when needed, or pass the slices as-is from the sequences.
// They might also be reused across iterations, which means that the caller
// should not retain the slices beyond the block of a for loop.
//
// For example, this code is incorrect:
//
//	var values [][]int
//	for vs, err := range kway.MergeSlice(seq0, seq1, seq2) {
//		if err != nil {
//			...
//		}
//		values = append(values, vs)
//	}
//	// Using values here may not contain the expected data, each slice might
//	// point to the same backing array and only contain values from the last
//	// iteration.
//
// Instead, the caller should copy the values into a new slice:
//
//	var values []int
//	for vs, err := range kway.MergeSlice(seq0, seq1, seq2) {
//		if err != nil {
//			...
//		}
//		values = append(values, vs...)
//	}
//
// Due to the increased complexity that derives from using MergeSlice,
// applications should prefer using Merge, which uses the same algorithm as
// MergeSlice internally, and can already achieve very decent throughput.
//
// See Merge for more details.
func MergeSlice[T cmp.Ordered](seqs ...iter.Seq2[[]T, error]) iter.Seq2[[]T, error] {
	return MergeSliceFunc(cmp.Compare[T], seqs...)
}

// MergeSliceFunc merges multiple sequences producing slices of ordered values
// using the given comparison function to determine the order. The sequences
// must be ordered by the same comparison function.
//
// See MergeSlice for more details.
func MergeSliceFunc[T any](cmp func(T, T) int, seqs ...iter.Seq2[[]T, error]) iter.Seq2[[]T, error] {
	switch len(seqs) {
	case 1:
		return seqs[0]
	case 2:
		return merge2(cmp, seqs[0], seqs[1])
	default:
		return merge(cmp, seqs)
	}
}

func buffer[T any](bufferSize int, seq iter.Seq2[T, error]) iter.Seq2[[]T, error] {
	buf := make([]T, bufferSize)
	return func(yield func([]T, error) bool) {
		n := 0

		var err error
		for buf[n], err = range seq {
			if err != nil {
				if !yield(nil, err) {
					return
				}
			} else if n++; n == len(buf) {
				if !yield(buf, nil) {
					return
				}
				n = 0
			}
		}

		if n > 0 {
			yield(buf[:n], nil)
		}
	}
}

func unbuffer[T any](seq iter.Seq2[[]T, error]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		seq(func(values []T, err error) bool {
			var value T
			if err != nil && !yield(value, err) {
				return false
			}
			for _, value = range values {
				if !yield(value, nil) {
					return false
				}
			}
			return true
		})
	}
}

func merge2[T any](cmp func(T, T) int, seq0, seq1 iter.Seq2[[]T, error]) iter.Seq2[[]T, error] {
	return func(yield func([]T, error) bool) {
		next0, stop0 := iter.Pull2(seq0)
		defer stop0()

		next1, stop1 := iter.Pull2(seq1)
		defer stop1()

		values0, err, ok0 := next0()
		if err != nil && !yield(nil, err) {
			return
		}

		values1, err, ok1 := next1()
		if err != nil && !yield(nil, err) {
			return
		}

		buffer := make([]T, bufferSize)
		offset := 0

		for ok0 && ok1 {
			i0 := 0
			i1 := 0

			for i0 < len(values0) && i1 < len(values1) {
				v0 := values0[i0]
				v1 := values1[i1]

				if (offset + 1) >= len(buffer) {
					if !yield(buffer[:offset], nil) {
						return
					}
					offset = 0
				}

				diff := cmp(v0, v1)
				switch {
				case diff < 0:
					buffer[offset] = v0
					offset++
					i0++
				case diff > 0:
					buffer[offset] = v1
					offset++
					i1++
				default:
					buffer[offset+0] = v0
					buffer[offset+1] = v1
					offset += 2
					i0++
					i1++
				}
			}

			if i0 == len(values0) {
				i0 = 0
				if values0, err, ok0 = next0(); err != nil && !yield(nil, err) {
					return
				}
			}

			if i1 == len(values1) {
				i1 = 0
				if values1, err, ok1 = next1(); err != nil && !yield(nil, err) {
					return
				}
			}
		}

		if offset > 0 && !yield(buffer[:offset], nil) {
			return
		}

		for ok0 && yield(values0, nil) {
			if values0, err, ok0 = next0(); err != nil && !yield(nil, err) {
				return
			}
		}

		for ok1 && yield(values1, nil) {
			if values1, err, ok1 = next1(); err != nil && !yield(nil, err) {
				return
			}
		}
	}
}

func merge[T any](cmp func(T, T) int, seqs []iter.Seq2[[]T, error]) iter.Seq2[[]T, error] {
	return func(yield func([]T, error) bool) {
		tree := makeTree(seqs...)
		defer tree.stop()

		buffer := make([]T, bufferSize)
		for {
			n, err := tree.next(buffer, cmp)
			if err == nil && n == 0 {
				return
			}
			if !yield(buffer[:n], err) {
				return
			}
		}
	}
}
