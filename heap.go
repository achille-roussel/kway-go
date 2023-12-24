package kway

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
