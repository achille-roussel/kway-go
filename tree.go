package kway

import "iter"

type tree[V any] struct {
	items  []iterator[V]
	nodes  []node
	count  int
	winner node
}

type node struct {
	index int
	value int
}

func makeTree[V any](seqs ...iter.Seq[V]) tree[V] {
	t := tree[V]{
		items:  make([]iterator[V], 0, len(seqs)),
		winner: node{index: -1, value: -1},
	}

	for _, seq := range seqs {
		buffer := make([]V, bufferSize)
		next, stop := bufferedPull(buffer, seq)
		v, ok := next()
		if ok {
			t.items = append(t.items, iterator[V]{
				item: v,
				next: next,
				stop: stop,
			})
		}
	}

	t.count = len(t.items)
	t.nodes = make([]node, 2*len(t.items))

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

func (t *tree[V]) initialize(i int, cmp func(V, V) int) node {
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

func (t *tree[V]) playGame(n1, n2 node, cmp func(V, V) int) (loser, winner node) {
	if n1.value < 0 {
		return n1, n2
	}
	if n2.value < 0 {
		return n2, n1
	}
	if cmp(t.items[n1.value].item, t.items[n2.value].item) < 0 {
		return n2, n1
	} else {
		return n1, n2
	}
}

func (t *tree[V]) next(cmp func(V, V) int) (value V, ok bool) {
	if t.count == 0 {
		return value, false
	}

	if t.winner.index < 0 {
		t.winner = t.initialize(0, cmp)
		return t.items[t.winner.value].item, true
	}

	it := &t.items[t.winner.value]
	v, ok := it.next()
	if ok {
		it.item = v
	} else {
		it.stop()
		t.nodes[t.winner.index] = node{index: -1, value: -1}
		t.count--
		t.winner.value = -1
		if t.count == 0 {
			return value, false
		}
	}

	winner := t.winner
	offset := parent(winner.index)
	for {
		player := t.nodes[offset]

		switch {
		case player.value < 0:
		case winner.value < 0:
			t.nodes[offset], winner = winner, player
		case cmp(t.items[player.value].item, t.items[winner.value].item) < 0:
			t.nodes[offset], winner = winner, player
		}

		if offset == 0 {
			t.winner = winner
			return t.items[t.winner.value].item, true
		}
		offset = parent(offset)
	}
}

func (t *tree[V]) stop() {
	for _, it := range t.items {
		it.stop()
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
