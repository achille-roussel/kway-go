package kway

import "iter"

//go:noinline
func bufferedFunc[V any](bufferSize int, seq iter.Seq[V]) iter.Seq[[]V] {
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

//go:noinline
func bufferedPull[V any](bufferSize int, seq iter.Seq[V]) (func() (V, bool), func()) {
	next, stop := iter.Pull(bufferedFunc(bufferSize, seq))

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
