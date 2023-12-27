package kway

import (
	"iter"
	"slices"
	"strings"
	"testing"
)

func words[T any](values ...T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, v := range values {
			if !yield(v) {
				return
			}
		}
	}
}

func TestTree(t *testing.T) {
	tests := []struct {
		scenario  string
		sequences [][]string
	}{
		{
			scenario:  "empty tree",
			sequences: [][]string{},
		},

		{
			scenario:  "three sequences with no elements",
			sequences: [][]string{{}, {}, {}},
		},

		{
			scenario:  "one sequence with one element",
			sequences: [][]string{{"a"}},
		},

		{
			scenario:  "one sequence with three elements",
			sequences: [][]string{{"a", "b", "c"}},
		},

		{
			scenario:  "three sequences with one element",
			sequences: [][]string{{"a"}, {"b"}, {"c"}},
		},

		{
			scenario: "three sequences of three elements",
			sequences: [][]string{
				{"a", "d", "g"},
				{"b", "e", "h"},
				{"c", "f", "i"},
			},
		},

		{
			scenario: "one sequence with the first element and a second sequence with the other elements",
			sequences: [][]string{
				{"a"},
				{"b", "c", "d", "e", "f", "g", "h", "i"},
			},
		},

		{
			scenario: "one sequence with the last element and a second sequence with the other elements",
			sequences: [][]string{
				{"z"},
				{"a", "b", "c", "d", "e", "f", "g", "h", "i"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			var seqs = make([]iter.Seq[string], len(test.sequences))
			for i, seq := range test.sequences {
				seqs[i] = words(seq...)
			}

			var tree = makeTree(seqs...)
			var values []string
			for {
				v, ok := tree.next(strings.Compare)
				if ok {
					values = append(values, v)
				} else {
					break
				}
			}

			var want []string
			for _, seq := range test.sequences {
				want = append(want, seq...)
			}
			slices.Sort(want)

			if !slices.Equal(values, want) {
				t.Errorf("expected replayed values to be in order, got %v, want %v", values, want)
			}
		})
	}
}

func TestParent(t *testing.T) {
	if p := parent((2 * 10) + 1); p != 10 {
		t.Errorf("expected parent of 21 to be 10, got %d", p)
	}
	if p := parent((2 * 10) + 2); p != 10 {
		t.Errorf("expected parent of 22 to be 10, got %d", p)
	}
}
