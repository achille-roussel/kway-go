// Package kway impements k-way merge algorithms for range functions.
package kway

import (
	"cmp"
	"fmt"
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

// Merge2 merges multiple sequences of key-value pairs into one. The sequences
// must produce ordered keys.
func Merge2[K cmp.Ordered, V any](seqs ...iter.Seq2[K, V]) iter.Seq2[K, V] {
	return Merge2Func(cmp.Compare[K], seqs...)
}

// MergeFunc merges multiple sequences into one using the given comparison
// function to determine the order of values. The sequences must be ordered
// by the same comparison function.
func MergeFunc[V any](cmp func(V, V) int, seqs ...iter.Seq[V]) iter.Seq[V] {
	switch len(seqs) {
	case 0:
		return merge0(cmp)
	case 1:
		return merge1(cmp, seqs[0])
	case 2:
		return merge2(cmp, seqs[0], seqs[1])
	default:
		return mergeN(cmp, seqs)
	}
}

// Merge2Func merges multiple sequences of key-value pairs into one using the
// given comparison function to determine the order of values. The sequences
// must be ordered by the same comparison function.
func Merge2Func[K, V any](cmp func(K, K) int, seqs ...iter.Seq2[K, V]) iter.Seq2[K, V] {
	pairs := make([]iter.Seq[pair[K, V]], 0, 8)
	for _, seq := range seqs {
		pairs = append(pairs, func(yield func(pair[K, V]) bool) {
			seq(func(k K, v V) bool { return yield(pair[K, V]{k, v}) })
		})
	}
	seq := MergeFunc(comparePairs[K, V](cmp), pairs...)
	return func(yield func(K, V) bool) { seq(func(p pair[K, V]) bool { return yield(p.k, p.v) }) }
}

type iterator[T any] struct {
	item T
	next func() (T, bool)
	stop func()
}

func (it *iterator[T]) String() string {
	if it == nil {
		return "<nil>"
	}
	return fmt.Sprint(it.item)
}

type pair[K, V any] struct {
	k K
	v V
}

func comparePairs[K, V any](cmp func(K, K) int) func(p1, p2 pair[K, V]) int {
	return func(p1, p2 pair[K, V]) int { return cmp(p1.k, p2.k) }
}

func merge0[V any](cmp func(V, V) int) iter.Seq[V] {
	return func(yield func(V) bool) {}
}

func merge1[V any](cmp func(V, V) int, seq0 iter.Seq[V]) iter.Seq[V] {
	return seq0
}

func merge2[V any](cmp func(V, V) int, seq0, seq1 iter.Seq[V]) iter.Seq[V] {
	return func(yield func(V) bool) {
		buf0 := make([]V, bufferSize)
		buf1 := make([]V, bufferSize)

		next0, stop0 := bufferedPull(buf0, seq0)
		defer stop0()

		next1, stop1 := bufferedPull(buf1, seq1)
		defer stop1()

		v0, ok0 := next0()
		v1, ok1 := next1()

		for ok0 && ok1 {
			var value V
			if cmp(v0, v1) < 0 {
				value = v0
				v0, ok0 = next0()
			} else {
				value = v1
				v1, ok1 = next1()
			}
			if !yield(value) {
				return
			}
		}

		if ok0 && yield(v0) {
			yieldAll(yield, next0)
		}
		if ok1 && yield(v1) {
			yieldAll(yield, next1)
		}
	}
}

func yieldAll[V any](yield func(V) bool, next func() (V, bool)) {
	var value V
	var ok = true
	for ok {
		if value, ok = next(); ok {
			ok = yield(value)
		}
	}
}

func mergeN[V any](cmp func(V, V) int, seqs []iter.Seq[V]) iter.Seq[V] {
	tree := makeTree(seqs...)
	return func(yield func(V) bool) {
		for tree.next(cmp) {
			if !yield(tree.top()) {
				break
			}
		}
	}
}
