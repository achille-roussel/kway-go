// Package kway impements k-way merge algorithms for range functions.
package kway

import (
	"cmp"
	"iter"
)

const (
	bufferSize = 128
)

// Merge merges multiple sequences into one. The sequences must produce ordered
// values.
func Merge[V cmp.Ordered](seqs ...iter.Seq[V]) iter.Seq[V] {
	return MergeFunc(cmp.Compare[V], seqs...)
}

// MergeFunc merges multiple sequences into one using the given comparison
// function to determine the order of values. The sequences must be ordered
// by the same comparison function.
func MergeFunc[V any](cmp func(V, V) int, seqs ...iter.Seq[V]) iter.Seq[V] {
	if len(seqs) == 0 {
		return func(func(V) bool) {}
	}
	if len(seqs) == 1 {
		return seqs[0]
	}
	var merged iter.Seq[[]V]
	if len(seqs) == 2 {
		seq0 := buffer(bufferSize, seqs[0])
		seq1 := buffer(bufferSize, seqs[1])
		merged = merge2(cmp, seq0, seq1)
	} else {
		bufferedSeqs := make([]iter.Seq[[]V], len(seqs))
		for i, seq := range seqs {
			bufferedSeqs[i] = buffer(bufferSize, seq)
		}
		merged = merge(cmp, bufferedSeqs)
	}
	return unbuffer(merged)
}

func MergeSlice[V cmp.Ordered](seqs ...iter.Seq[[]V]) iter.Seq[[]V] {
	return MergeSliceFunc(cmp.Compare[V], seqs...)
}

func MergeSliceFunc[V any](cmp func(V, V) int, seqs ...iter.Seq[[]V]) iter.Seq[[]V] {
	switch len(seqs) {
	case 0:
		return func(func([]V) bool) {}
	case 1:
		return seqs[0]
	case 2:
		return merge2(cmp, seqs[0], seqs[1])
	default:
		return merge(cmp, seqs)
	}
}

func buffer[V any](bufferSize int, seq iter.Seq[V]) iter.Seq[[]V] {
	buf := make([]V, bufferSize)
	return func(yield func([]V) bool) {
		n := 0

		for buf[n] = range seq {
			if n++; n == len(buf) {
				if !yield(buf) {
					return
				}
				n = 0
			}
		}

		if n > 0 {
			yield(buf[:n])
		}
	}
}

func unbuffer[V any](seq iter.Seq[[]V]) iter.Seq[V] {
	return func(yield func(V) bool) {
		seq(func(values []V) bool {
			for _, value := range values {
				if !yield(value) {
					return false
				}
			}
			return true
		})
	}
}

func merge2[V any](cmp func(V, V) int, seq0, seq1 iter.Seq[[]V]) iter.Seq[[]V] {
	return func(yield func([]V) bool) {
		next0, stop0 := iter.Pull(seq0)
		defer stop0()

		next1, stop1 := iter.Pull(seq1)
		defer stop1()

		values0, ok0 := next0()
		values1, ok1 := next1()

		buffer := make([]V, bufferSize)
		offset := 0

		for ok0 && ok1 {
			i0 := 0
			i1 := 0

			for i0 < len(values0) && i1 < len(values1) {
				v0 := values0[i0]
				v1 := values1[i1]

				if (offset + 1) >= len(buffer) {
					if !yield(buffer[:offset]) {
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
				values0, ok0 = next0()
			}

			if i1 == len(values1) {
				i1 = 0
				values1, ok1 = next1()
			}
		}

		if offset > 0 && !yield(buffer[:offset]) {
			return
		}

		for ok0 && yield(values0) {
			values0, ok0 = next0()
		}

		for ok1 && yield(values1) {
			values1, ok1 = next1()
		}
	}
}

func merge[V any](cmp func(V, V) int, seqs []iter.Seq[[]V]) iter.Seq[[]V] {
	return func(yield func([]V) bool) {
		tree := makeTree(seqs...)
		defer tree.stop()

		buffer := make([]V, bufferSize)
		for {
			n := tree.next(buffer, cmp)
			if n == 0 {
				return
			}
			if !yield(buffer[:n]) {
				return
			}
		}
	}
}
