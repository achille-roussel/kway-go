// Package kway impements k-way merge algorithms for range functions.
package kway

import (
	"cmp"
	"iter"
)

const (
	bufferSize = 128
)

func BulkMerge[V cmp.Ordered](seqs ...iter.Seq[[]V]) iter.Seq[V] {
	return BulkMergeFunc(cmp.Compare[V], seqs...)
}

func BulkMergeFunc[V any](cmp func(V, V) int, seqs ...iter.Seq[[]V]) iter.Seq[V] {
	switch len(seqs) {
	case 0:
		return merge0[V]()
	case 1:
		return merge1(seqs[0])
	case 2:
		return merge2(cmp, seqs[0], seqs[1])
	default:
		return mergeN(cmp, seqs)
	}
}

// Merge merges multiple sequences into one. The sequences must produce ordered
// values.
func Merge[V cmp.Ordered](seqs ...iter.Seq[V]) iter.Seq[V] {
	return MergeFunc(cmp.Compare[V], seqs...)
}

// MergeFunc merges multiple sequences into one using the given comparison
// function to determine the order of values. The sequences must be ordered
// by the same comparison function.
func MergeFunc[V any](cmp func(V, V) int, seqs ...iter.Seq[V]) iter.Seq[V] {
	switch len(seqs) {
	case 0:
		return merge0[V]()
	case 1:
		return seqs[0]
	case 2:
		seq0 := bufferedFunc(bufferSize, seqs[0])
		seq1 := bufferedFunc(bufferSize, seqs[1])
		return merge2(cmp, seq0, seq1)
	default:
		bufferedSeqs := make([]iter.Seq[[]V], len(seqs))
		for i, seq := range seqs {
			bufferedSeqs[i] = bufferedFunc(bufferSize, seq)
		}
		return mergeN(cmp, bufferedSeqs)
	}
}

//go:noinline
func merge0[V any]() iter.Seq[V] {
	return func(func(V) bool) {}
}

//go:noinline
func merge1[V any](seq iter.Seq[[]V]) iter.Seq[V] {
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

//go:noinline
func merge2[V any](cmp func(V, V) int, seq0, seq1 iter.Seq[[]V]) iter.Seq[V] {
	return func(yield func(V) bool) {
		next0, stop0 := iter.Pull(seq0)
		defer stop0()

		next1, stop1 := iter.Pull(seq1)
		defer stop1()

		values0, ok0 := next0()
		values1, ok1 := next1()

		for ok0 && ok1 {
			i0 := 0
			i1 := 0

			for i0 < len(values0) && i1 < len(values1) {
				v0 := values0[i0]
				v1 := values1[i1]

				diff := cmp(v0, v1)
				cont := false
				switch {
				case diff < 0:
					cont = yield(v0)
					i0++
				case diff > 0:
					cont = yield(v1)
					i1++
				default:
					cont = yield(v0) && yield(v1)
					i0++
					i1++
				}
				if !cont {
					return
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

		flush(yield, next0, values0, ok0)
		flush(yield, next1, values1, ok1)
	}
}

func flush[V any](yield func(V) bool, next func() ([]V, bool), values []V, ok bool) {
	for ok {
		for _, value := range values {
			if !yield(value) {
				return
			}
		}
		values, ok = next()
	}
}

//go:noinline
func mergeN[V any](cmp func(V, V) int, seqs []iter.Seq[[]V]) iter.Seq[V] {
	return func(yield func(V) bool) {
		tree := makeTree(seqs...)
		defer tree.stop()

		var value V
		var ok = true
		for ok {
			if value, ok = tree.next(cmp); ok {
				ok = yield(value)
			}
		}
	}
}
