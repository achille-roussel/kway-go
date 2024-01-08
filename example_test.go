package kway_test

import (
	"fmt"
	"iter"

	"github.com/achille-roussel/kway-go"
)

func ExampleMerge() {
	sequence := func(min, max, step int) iter.Seq2[int, error] {
		return func(yield func(int, error) bool) {
			for i := min; i < max; i += step {
				if !yield(i, nil) {
					return
				}
			}
		}
	}

	for value, err := range kway.Merge(
		sequence(0, 5, 1), // 0,1,2,3,4
		sequence(1, 5, 2), // 1,3
		sequence(2, 5, 3), // 2
	) {
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v,", value)
	}

	// Output:
	// 0,1,1,2,2,3,3,4,
}

func ExampleMergeSlice() {
	sequence := func(min, max, step, size int) iter.Seq2[[]int, error] {
		return func(yield func([]int, error) bool) {
			values := make([]int, size)
			for i := min; i < max; i += step {
				for j := range values {
					values[j] = i + j
				}
				if !yield(values, nil) {
					return
				}
			}
		}
	}

	for values, err := range kway.MergeSlice(
		sequence(0, 5, 1, 2), // [0,1],[1,2],[2,3],[3,4],[4,5]
		sequence(1, 5, 2, 2), // [1,2],[3,4]
		sequence(2, 5, 3, 2), // [2,3]
	) {
		if err != nil {
			panic(err)
		}
		for _, value := range values {
			fmt.Printf("%v,", value)
		}
	}

	// Output:
	// 0,1,1,1,2,2,2,2,3,3,3,3,4,4,4,5,
}
