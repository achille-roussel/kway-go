package kway

import (
	"iter"
)

type tree[T any] struct {
	cursors []cursor[T]
	nodes   []node
	count   int
	winner  node
	// challenger is the index of the cursor holding the runner-up to the
	// winner (the second smallest head overall), or -1 when unknown. It
	// bounds the run of values that the winner can produce without the
	// tree being replayed.
	challenger int
	// runMode selects the strategy used to fill buffers: nextRuns amortizes
	// tree replays over runs of consecutive values won by the same cursor,
	// while nextScalar replays the tree for each value, which is faster when
	// values from the sequences are interleaved.
	runMode bool
	// pending is set after a batch of the winning cursor was passed through
	// to the caller without being copied. The cursor must not be refilled
	// until the caller is done with the batch, because refilling resumes the
	// producer, which may reuse the memory holding the values; the refill is
	// performed at the beginning of the next call to next.
	pending bool
}

type node struct {
	index int
	value int
}

type cursor[T any] struct {
	values []T
	err    error
	next   func() ([]T, error, bool)
	stop   func()
}

func makeTree[T any](seqs ...iter.Seq2[[]T, error]) tree[T] {
	t := tree[T]{
		cursors:    make([]cursor[T], len(seqs)),
		winner:     node{index: -1, value: -1},
		challenger: -1,
	}

	for i, seq := range seqs {
		next, stop := iter.Pull2(seq)
		t.cursors[i] = cursor[T]{next: next, stop: stop}
	}

	t.count = len(t.cursors)
	t.nodes = make([]node, 2*len(t.cursors))

	head := t.nodes[:len(t.nodes)/2]
	tail := t.nodes[len(t.nodes)/2:]

	for i := range head {
		head[i] = node{index: -1, value: -1}
	}
	for i := range tail {
		tail[i] = node{index: i + len(tail), value: i}
	}
	return t
}

func (t *tree[T]) initialize(i int, cmp func(T, T) int) node {
	if i >= len(t.nodes) {
		return node{index: -1, value: -1}
	}
	n1 := t.initialize(left(i), cmp)
	n2 := t.initialize(right(i), cmp)
	if n1.index < 0 && n2.index < 0 {
		return t.nodes[i]
	}
	loser, winner := t.playGame(n1, n2, cmp)
	t.nodes[i] = loser
	return winner
}

func (t *tree[T]) playGame(n1, n2 node, cmp func(T, T) int) (loser, winner node) {
	if n1.value < 0 {
		return n1, n2
	}
	if n2.value < 0 {
		return n2, n1
	}
	c1 := &t.cursors[n1.value]
	c2 := &t.cursors[n2.value]
	if c1.err != nil {
		return n2, n1
	}
	if c2.err != nil {
		return n1, n2
	}
	if cmp(c1.values[0], c2.values[0]) < 0 {
		return n2, n1
	} else {
		return n1, n2
	}
}

// next produces the next batch of merged values, either by filling buf and
// returning a prefix of it, or by returning a batch taken directly from one
// of the sequences when all its values are known to sort before the heads of
// every other sequence. It returns an empty batch with a nil error when all
// the sequences have been consumed.
func (t *tree[T]) next(buf []T, cmp func(T, T) int) (values []T, err error) {
	if len(buf) == 0 || t.count == 0 {
		return nil, nil
	}

	if t.winner.index < 0 {
		for i := range t.cursors {
			c := &t.cursors[i]
			values, err, ok := nextNonEmptyValues(c.next)
			if ok {
				c.values, c.err = values, err
			} else {
				c.stop()
				t.nodes[i+len(t.cursors)] = node{index: -1, value: -1}
				t.count--
				continue
			}
		}
		if t.count == 0 {
			return nil, nil
		}
		t.winner = t.initialize(0, cmp)
	}

	if t.pending {
		// The previous call passed a batch of the winning cursor through
		// without copying it; the caller is now done with it, so the cursor
		// can be refilled and the tree replayed.
		c := &t.cursors[t.winner.value]
		if err := c.err; err != nil {
			c.err = nil
			return nil, err
		}
		t.pending = false
		winner := t.winner
		values, err, ok := nextNonEmptyValues(c.next)
		if ok {
			c.values, c.err = values, err
		} else {
			c.stop()
			winner.value = -1
			t.nodes[winner.index] = node{index: -1, value: -1}
			if t.count--; t.count == 0 {
				t.winner = winner
				return nil, nil
			}
		}
		t.winner, t.challenger = t.replay(winner, cmp)
	}

	// Zero-copy fast path: a batch of the winning cursor is passed through
	// as-is when its last value sorts before the head of every other
	// sequence, which takes a single comparison against the challenger.
	// Small batches are excluded so sequences producing tiny batches get
	// aggregated into the buffer instead of degrading into small yields.
	c := &t.cursors[t.winner.value]
	if n := len(c.values); n >= minBufferSize {
		wholeBatch := t.count == 1
		if !wholeBatch {
			if ch := t.challenger; ch >= 0 {
				wholeBatch = cmp(c.values[n-1], t.cursors[ch].values[0]) <= 0
			}
		}
		if wholeBatch {
			values := c.values
			c.values = nil
			t.pending = true
			return values, nil
		}
	}

	var n int
	if t.runMode {
		n, err = t.nextRuns(buf, cmp)
	} else {
		n, err = t.nextScalar(buf, cmp)
	}
	return buf[:n], err
}

// nextScalar pops one value per tree replay; this is the fastest strategy
// when values from the sequences are interleaved. It counts how often the
// same cursor wins consecutively to detect run-structured inputs, which are
// better served by nextRuns.
func (t *tree[T]) nextScalar(buf []T, cmp func(T, T) int) (n int, err error) {
	winner := t.winner
	same := 0
	prev := -1

	for n < len(buf) {
		c := &t.cursors[winner.value]

		if len(c.values) > 0 {
			buf[n] = c.values[0]
			n++
			c.values = c.values[1:]
		}
		if winner.value == prev {
			same++
		}
		prev = winner.value

		if len(c.values) == 0 {
			if err = c.err; err != nil {
				c.err = nil
				break
			}
			values, err, ok := nextNonEmptyValues(c.next)
			if ok {
				c.values, c.err = values, err
			} else {
				c.stop()
				winner.value = -1
				t.nodes[winner.index] = node{index: -1, value: -1}
				if t.count--; t.count == 0 {
					break
				}
			}
		}

		for offset := parent(winner.index); true; offset = parent(offset) {
			player := t.nodes[offset]

			if player.value >= 0 {
				if winner.value < 0 {
					t.nodes[offset], winner = winner, player
				} else {
					c1 := &t.cursors[player.value]
					c2 := &t.cursors[winner.value]
					if len(c1.values) == 0 || (len(c2.values) != 0 && cmp(c1.values[0], c2.values[0]) < 0) {
						t.nodes[offset], winner = winner, player
					}
				}
			}

			if offset == 0 {
				break
			}
		}
	}

	t.winner = winner
	t.runMode = same > n/2
	t.challenger = -1
	return n, err
}

// nextRuns amortizes tree replays over runs of consecutive values produced by
// the same cursor: every winner value that sorts before the challenger is
// emitted without replaying the tree. It falls back to nextScalar when the
// average run length drops below 3/2.
func (t *tree[T]) nextRuns(buf []T, cmp func(T, T) int) (n int, err error) {
	winner := t.winner
	pops := 0

	for n < len(buf) {
		c := &t.cursors[winner.value]

		if len(c.values) > 0 {
			limit := c.values
			if max := len(buf) - n; len(limit) > max {
				limit = limit[:max]
			}
			run := 1
			if t.count == 1 {
				run = len(limit)
			} else if ch := t.challenger; ch >= 0 {
				challengerHead := t.cursors[ch].values[0]
				for run < len(limit) && cmp(limit[run], challengerHead) <= 0 {
					run++
				}
			}
			if run == 1 {
				buf[n] = limit[0]
				n++
			} else {
				n += copy(buf[n:], limit[:run])
			}
			c.values = c.values[run:]
		}
		pops++

		if len(c.values) == 0 {
			if err = c.err; err != nil {
				c.err = nil
				break
			}
			values, err, ok := nextNonEmptyValues(c.next)
			if ok {
				c.values, c.err = values, err
			} else {
				c.stop()
				winner.value = -1
				t.nodes[winner.index] = node{index: -1, value: -1}
				if t.count--; t.count == 0 {
					break
				}
			}
		}

		winner, t.challenger = t.replay(winner, cmp)
	}

	t.winner = winner
	t.runMode = n > pops+pops/2
	return n, err
}

// replay walks the loser tree from the winner's leaf to the root, replaying
// the games after the head value of the winning cursor changed. It returns
// the new winner, along with the index of the cursor holding the runner-up
// to the winner, or -1 when it could not be determined.
func (t *tree[T]) replay(winner node, cmp func(T, T) int) (node, int) {
	challenger := -1
	challengerOK := true
	startValue := winner.value

	for offset := parent(winner.index); true; offset = parent(offset) {
		player := t.nodes[offset]

		if player.value >= 0 {
			if winner.value < 0 {
				t.nodes[offset], winner = winner, player
			} else {
				c1 := &t.cursors[player.value]
				c2 := &t.cursors[winner.value]
				if len(c1.values) == 0 || (len(c2.values) != 0 && cmp(c1.values[0], c2.values[0]) < 0) {
					t.nodes[offset], winner = winner, player
				}
			}
			// The node now holds the loser of this game. By the tournament
			// property, the smallest loser head along the winner's path is
			// the second smallest value overall. Cursors with no values
			// (holding a pending error) invalidate the challenger since
			// their position in the order is unknown.
			if l := t.nodes[offset].value; l >= 0 {
				lc := &t.cursors[l]
				if len(lc.values) == 0 {
					challengerOK = false
				} else if challenger < 0 || cmp(lc.values[0], t.cursors[challenger].values[0]) < 0 {
					challenger = l
				}
			}
		}

		if offset == 0 {
			break
		}
	}

	// The challenger is only valid if the walked path belongs to the final
	// winner: when the winner changes during the replay, the runner-up may
	// live in the new winner's own subtree, which was not visited.
	if !challengerOK || winner.value != startValue {
		challenger = -1
	}
	return winner, challenger
}

func (t *tree[T]) stop() {
	for _, c := range t.cursors {
		c.stop()
	}
}

func parent(i int) int {
	return (i - 1) / 2
}

func left(i int) int {
	return (2 * i) + 1
}

func right(i int) int {
	return (2 * i) + 2
}

func nextNonEmptyValues[T any](next func() ([]T, error, bool)) (values []T, err error, ok bool) {
	for {
		values, err, ok = next()
		if len(values) > 0 || err != nil || !ok {
			return values, err, ok
		}
	}
}
