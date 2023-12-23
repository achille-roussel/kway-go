// Package kway impements k-way merge algorithms for range functions.
package kway

import (
	"cmp"
	"iter"
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
		next0, stop0 := iter.Pull(seq0)
		defer stop0()

		next1, stop1 := iter.Pull(seq1)
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

		if ok0 {
			if !yield(v0) {
				return
			}
			seq0(yield)
		}

		if ok1 {
			if !yield(v1) {
				return
			}
			seq1(yield)
		}
	}
}

func mergeN[V any](cmp func(V, V) int, seqs []iter.Seq[V]) iter.Seq[V] {
	heap := make([]iterator[V], len(seqs))

	for i, seq := range seqs {
		next, stop := iter.Pull(seq)
		heap[i] = iterator[V]{next: next, stop: stop}
	}

	return func(yield func(V) bool) {
		defer func() {
			for i := range heap {
				heap[i].stop()
			}
		}()
		i := 0

		for j := range heap {
			if v, ok := heap[j].next(); ok {
				heap[i] = heap[j]
				heap[i].item = v
				i++
			} else {
				heap[j].stop()
				heap[j].next = nil
			}
		}

		heap = heap[:i]
		heapify(heap, cmp)

		for len(heap) > 0 {
			m := &heap[0]
			if !yield(m.item) {
				return
			}
			v, ok := m.next()
			if ok {
				m.item = v
				fix(heap, 0, cmp)
			} else {
				m.stop()
				heap = pop(heap, cmp)
			}
		}
	}
}

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

func heapify[T any](h []iterator[T], cmp func(T, T) int) {
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		down(h, i, n, cmp)
	}
}

func pop[T any](h []iterator[T], cmp func(T, T) int) []iterator[T] {
	n := len(h) - 1
	h[0], h[n] = h[n], h[0]
	down(h, 0, n, cmp)
	return h[:n]
}

func fix[T any](h []iterator[T], i int, cmp func(T, T) int) {
	if !down(h, i, len(h), cmp) {
		up(h, i, cmp)
	}
}

func up[T any](h []iterator[T], j int, cmp func(T, T) int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || cmp(h[j].item, h[i].item) >= 0 {
			break
		}
		h[i], h[j] = h[j], h[i]
		j = i
	}
}

func down[T any](h []iterator[T], i0, n int, cmp func(T, T) int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && cmp(h[j2].item, h[j1].item) < 0 {
			j = j2 // = 2*i + 2  // right child
		}
		if cmp(h[j].item, h[i].item) >= 0 {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
	return i > i0
}
