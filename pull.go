package kway

import "iter"

//go:noinline
func bufferedFunc[V any](buf []V, seq iter.Seq[V]) iter.Seq[[]V] {
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

//go:noinline
func bufferedPull[V any](buf []V, seq iter.Seq[V]) (func() (V, bool), func()) {
	next, stop := iter.Pull(bufferedFunc(buf, seq))

	var values []V
	var offset int

	return func() (value V, ok bool) {
		for {
			if offset < len(values) {
				value = values[offset]
				offset++
				return value, true
			}
			values, ok = next()
			if !ok {
				return value, false
			}
			offset = 0
		}
	}, stop
}
