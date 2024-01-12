package kway

import (
	"iter"
)

type tree[T any] struct {
	cursors []cursor[T]
	nodes   []node
	count   int
	winner  node
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
		cursors: make([]cursor[T], len(seqs)),
		winner:  node{index: -1, value: -1},
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

func (t *tree[T]) next(buf []T, cmp func(T, T) int) (n int, err error) {
	if len(buf) == 0 || t.count == 0 {
		return 0, nil
	}

	winner := t.winner
	if winner.index < 0 {
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
			return 0, nil
		}
		winner = t.initialize(0, cmp)
	}

	for n < len(buf) {
		c := &t.cursors[winner.value]

		if len(c.values) > 0 {
			buf[n] = c.values[0]
			n++
			c.values = c.values[1:]
		}

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
				t.count--
				if t.count == 0 {
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
	return n, err
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
