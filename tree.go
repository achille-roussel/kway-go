package kway

import "iter"

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
	next   func() ([]T, error, bool)
	stop   func()
}

func buildTree[T any](seqs ...iter.Seq2[[]T, error]) (tree[T], error) {
	t := tree[T]{
		cursors: make([]cursor[T], 0, len(seqs)),
		winner:  node{index: -1, value: -1},
	}

	for _, seq := range seqs {
		next, stop := iter.Pull2(seq)
		values, err, ok := nextNonEmptyValues(next)
		if err != nil {
			return t, err
		}
		if ok {
			t.cursors = append(t.cursors, cursor[T]{
				values: values,
				next:   next,
				stop:   stop,
			})
		} else {
			stop()
		}
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
	return t, nil
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
	if cmp(t.cursors[n1.value].values[0], t.cursors[n2.value].values[0]) < 0 {
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
		winner = t.initialize(0, cmp)
		buf[n] = t.cursors[winner.value].values[0]
		n++
	}

	for n < len(buf) {
		c := &t.cursors[winner.value]
		c.values = c.values[1:]

		if len(c.values) == 0 {
			values, err, ok := nextNonEmptyValues(c.next)
			if err != nil {
				return n, err
			} else if ok {
				c.values = values
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

		offset := parent(winner.index)
		for {
			player := t.nodes[offset]

			switch {
			case player.value < 0:
			case winner.value < 0:
				t.nodes[offset], winner = winner, player
			case cmp(t.cursors[player.value].values[0], t.cursors[winner.value].values[0]) < 0:
				t.nodes[offset], winner = winner, player
			}

			if offset == 0 {
				break
			}

			offset = parent(offset)
		}

		buf[n] = t.cursors[winner.value].values[0]
		n++
	}

	t.winner = winner
	return n, nil
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

func nextNonEmptyValues[T any](next func() ([]T, error, bool)) ([]T, error, bool) {
	for {
		values, err, ok := next()
		if len(values) > 0 || err != nil || !ok {
			return values, err, ok
		}
	}
}
