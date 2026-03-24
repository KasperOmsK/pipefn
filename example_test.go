package pipefn_test

import (
	"fmt"
	"math"

	"github.com/KasperOmsK/pipefn"
)

func ExampleFromChan() {
	ch := make(chan int)

	p, done := pipefn.FromChan(ch)

	// Producer
	go func() {
		defer close(ch)
		for i := 0; i < 10; i++ {
			select {
			case ch <- i:
			case <-done:
				fmt.Println("producer: stopping early")
				return
			}
		}

	}()

	i := 0
	// Consumer: only take first 3 values
	for v := range p.Values().Seq() {
		fmt.Println(v)
		i++
		if i >= 3 {
			break
		}
	}
	// wait for producer to stop to ensure that the output
	// is consistent
	<-ch

	// Output:
	// 0
	// 1
	// 2
	// producer: stopping early
}

func ExampleTake() {
	input := pipefn.FromSlice([]int{1, 2, 3, 4, 5})
	take := pipefn.Take(input, 3)
	values, _, _ := take.Collect()

	for _, v := range values {
		fmt.Println(v)
	}
	// Output:
	// 1
	// 2
	// 3
}

func ExampleDrop() {
	input := pipefn.FromSlice([]int{1, 2, 3, 4, 5})
	take := pipefn.Drop(input, 3)
	values, _, _ := take.Collect()

	for _, v := range values {
		fmt.Println(v)
	}
	// Output:
	// 4
	// 5
}

func ExampleMap() {
	input := pipefn.FromSlice([]int{1, 2, 3})

	squared := pipefn.Map(input, func(in int) int { return in * in })

	values, _, _ := squared.Collect()
	for _, v := range values {
		fmt.Println(v)
	}
	// Output:
	// 1
	// 4
	// 9
}

func ExampleTryMap() {

	input := pipefn.FromSlice([]int{1, -2, 16})

	squared := pipefn.TryMap(input, func(in int) (float64, error) {
		if in < 0 {
			return 0, fmt.Errorf("NaN")
		}
		return math.Sqrt(float64(in)), nil
	})

	values, errs, _ := squared.Collect()
	for _, v := range values {
		fmt.Println(v)
	}

	for _, e := range errs {
		perr := e.(*pipefn.PipeError)
		fmt.Printf("error: item=%v, reason=%s\n", perr.Item, perr.Reason)
	}

	// Output:
	// 1
	// 4
	// error: item=-2, reason=NaN
}

func ExampleFlatten() {
	groups := pipefn.FromSlice([][]int{{1, 2}, {3, 4}})

	flattened := pipefn.Flatten(groups)
	values, _, _ := flattened.Collect()

	for _, v := range values {
		fmt.Println(v)
	}
	// Output:
	// 1
	// 2
	// 3
	// 4
}

func ExampleFlatMap() {
	input := pipefn.FromSlice([]int{1, 2, 3})

	// For each number, produce a slice of the number and multiple of 10
	flatMapped := pipefn.FlatMap(input, func(in int) []int {
		return []int{in, in * 10}
	})

	values, _, _ := flatMapped.Collect()
	for _, v := range values {
		fmt.Println(v)
	}
	// Output:
	// 1
	// 10
	// 2
	// 20
	// 3
	// 30
}

func ExampleFlatTryMap() {
	input := pipefn.FromSlice([]int{1, -2, 3})

	// For each number, produce a slice of the number and multiple of 10 if positive
	flatTryMapped := pipefn.FlatTryMap(input, func(in int) ([]int, error) {
		if in < 0 {
			return nil, fmt.Errorf("negative number")
		}
		return []int{in, in * 10}, nil
	})

	values, errs, _ := flatTryMapped.Collect()

	for _, v := range values {
		fmt.Println(v)
	}

	for _, e := range errs {
		perr := e.(*pipefn.PipeError)
		fmt.Printf("error: item=%v, reason=%s\n", perr.Item, perr.Reason)
	}

	// Output:
	// 1
	// 10
	// 3
	// 30
	// error: item=-2, reason=negative number
}

func ExampleFilter() {
	input := pipefn.FromSlice([]int{1, 2, 3, 4, 5})

	// Keep only even numbers
	filtered := pipefn.Filter(input, func(n int) bool {
		return n%2 == 0
	})

	values, _, _ := filtered.Collect()
	for _, v := range values {
		fmt.Println(v)
	}

	// Output:
	// 2
	// 4
}

func ExampleGroupByKey() {

	input := pipefn.FromSlice([]string{"apple", "apricot", "banana", "blueberry", "cherry", "avocado"})

	// Group strings by their first letter
	grouped := pipefn.GroupByKey(input, func(s string) string {
		return string(s[0])
	})

	values, _, _ := grouped.Collect()
	for _, group := range values {
		fmt.Println(group)
	}

	// Note that since the input is not preordered by grouping key, "avocado" appears after "cherry"

	// Output:
	// [apple apricot]
	// [banana blueberry]
	// [cherry]
	// [avocado]
}

func ExampleGroupByKeyAggregate() {
	input := pipefn.FromSlice(
		[]int{
			1, 2,
			10, 10, 10,
			20, 25,
			5,
		})

	// Group by tens digit and sum values within each consecutive group
	keyFunc := func(v int) int {
		return v / 10 // key = 10s digit
	}
	initFunc := func(_ int) int {
		return 0 // start sum at 0
	}
	updateFunc := func(acc *int, v int) {
		*acc += v // add the value to the accumulator
	}

	grouped := pipefn.GroupByKeyAggregate(input, keyFunc, initFunc, updateFunc)

	values, _, _ := grouped.Collect()
	for _, v := range values {
		fmt.Println(v)
	}

	// Note that since the input is not preordered by grouping key, the last single digit value appears at the end

	// Output:
	// 3
	// 30
	// 45
	// 5
}

func ExampleChunk() {
	flat := pipefn.FromSlice([]int{1, 2, 3, 4, 5})

	chunked := pipefn.Chunk(flat, 2)

	values, _, _ := chunked.Collect()

	for _, v := range values {
		fmt.Println(v)
	}
	// Output:
	// [1 2]
	// [3 4]
	// [5]
}

func ExampleMerge() {
	p1 := pipefn.FromSlice([]int{1, 2})
	p2 := pipefn.FromSlice([]int{3})
	p3 := pipefn.FromSlice([]int{4, 5})

	merged := pipefn.Merge(p1, p2, p3)

	values, _, _ := merged.Collect()

	for _, v := range values {
		fmt.Println(v)
	}

	// Possible output:
	// 1
	// 3
	// 2
	// 4
	// 5
}

func ExampleConcat() {
	p1 := pipefn.FromSlice([]int{1, 2})
	p2 := pipefn.FromSlice([]int{3})
	p3 := pipefn.FromSlice([]int{4, 5})

	concat := pipefn.Concat(p1, p2, p3)

	values, _, _ := concat.Collect()

	// Print concatenated values
	for _, v := range values {
		fmt.Println(v)
	}

	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
}

func ExampleZip() {
	p1 := pipefn.FromSlice([]int{1, 2, 3, 4})
	p2 := pipefn.FromSlice([]string{"a", "b", "c"})

	zipped := pipefn.Zip(p1, p2)

	values, _, _ := zipped.Collect()

	for _, v := range values {
		fmt.Printf("left=%d, right=%s\n", v.LValue, v.RValue)
	}

	// Output:
	// left=1, right=a
	// left=2, right=b
	// left=3, right=c
}
