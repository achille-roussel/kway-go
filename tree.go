package kway

import (
	"fmt"
	"iter"

	"github.com/xlab/treeprint"
)

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

func (n node) String() string {
	return fmt.Sprintf("{%d:%d}", n.index, n.value)
}

func makeTree[V any](seqs ...iter.Seq[V]) tree[V] {
	t := tree[V]{
		items:  make([]iterator[V], 0, len(seqs)),
		winner: node{index: -1, value: -1},
	}

	for _, seq := range seqs {
		next, stop := iter.Pull(seq)
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
	for i := range t.items {
		t.nodes[i] = node{index: -1, value: -1}
	}
	for i := range t.items {
		j := i + len(t.items)
		t.nodes[j] = node{index: j, value: i}
	}
	return t
}

func (t tree[V]) String() string {
	p := treeprint.New()
	t.print(p, 0)
	return p.String()
}

func (t tree[V]) print(p treeprint.Tree, i int) {
	if i >= len(t.nodes) {
		return
	}
	var s string
	if n := t.nodes[i]; n.value < 0 {
		s = fmt.Sprintf("%d: nil", i)
	} else {
		s = fmt.Sprintf("%d: %v", i, t.items[n.value].item)
	}
	if i == 0 {
		p.SetValue(s)
	} else {
		p = p.AddBranch(s)
	}
	t.print(p, left(i))
	t.print(p, right(i))
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

func (t *tree[V]) replayGames(winner node, cmp func(V, V) int) node {
	i := parent(winner.index)
	for {
		t.nodes[i], winner = t.playGame(t.nodes[i], winner, cmp)
		if i == 0 {
			return winner
		}
		i = parent(i)
	}
}

func (t *tree[V]) playGame(n1, n2 node, cmp func(V, V) int) (loser, winner node) {
	if n1.value < 0 {
		return n1, n2
	}
	if n2.value < 0 {
		return n2, n1
	}
	i1 := t.items[n1.value]
	i2 := t.items[n2.value]
	if cmp(i1.item, i2.item) < 0 {
		return n2, n1
	} else {
		return n1, n2
	}
}

func (t *tree[V]) next(cmp func(V, V) int) bool {
	if t.count == 0 {
		return false
	}

	if t.winner.index < 0 {
		t.winner = t.initialize(0, cmp)
		return true
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
			return false
		}
	}

	t.winner = t.replayGames(t.winner, cmp)
	return true
}

func (t *tree[V]) len() int {
	return t.count
}

func (t *tree[V]) top() V {
	var zero V
	if t.winner.value < 0 {
		return zero
	}
	return t.items[t.winner.value].item
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
