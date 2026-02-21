package pipefn_test

import (
	"fmt"
	"iter"
	"pipefn"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMap_TransformsValues(t *testing.T) {
	src := pipefn.From(seqOf(1, 2, 3))

	p := pipefn.Map(src, func(v int) int {
		return v * 2
	})

	vals, errs := collect(p)

	require.Equal(t, []int{2, 4, 6}, vals)
	require.Empty(t, errs)
}

func TestTryMap_ForwardsErrors(t *testing.T) {
	src := pipefn.From(seqOf(1, 2, 3, 4))

	p := pipefn.TryMap(src, func(v int) (int, error) {
		if v%2 == 0 {
			return 0, fmt.Errorf("even number: %d", v)
		}
		return v * 10, nil
	})

	vals, errs := collect(p)

	require.Equal(t, []int{10, 30}, vals)
	require.Len(t, errs, 2)
}

func TestFilter_FiltersCorrectly(t *testing.T) {
	src := pipefn.From(seqOf(1, 2, 3, 4, 5))

	p := pipefn.Filter(src, func(v int) bool {
		return v%2 == 0
	})

	vals, errs := collect(p)

	require.Equal(t, []int{2, 4}, vals)
	require.Empty(t, errs)
}

func TestFilter_ForwardsErrors(t *testing.T) {
	// Input sequence
	input := pipefn.From(seqOf(1, 2, 3, 4, 5))

	// Inject errors for some values using TryMap
	inputWithErr := pipefn.TryMap(input, func(v int) (int, error) {
		if v%2 == 0 {
			return 0, fmt.Errorf("even number: %d", v)
		}
		return v, nil
	})

	// Filter values greater than 2
	filtered := pipefn.Filter(inputWithErr, func(v int) bool {
		return v > 2
	})

	vals, errs := collect(filtered)

	// Only odd values greater than 2 should remain
	require.Equal(t, []int{3, 5}, vals)

	// Errors should include the ones for even numbers
	require.Len(t, errs, 2)
	errorMsgs := []string{errs[0].Reason.Error(), errs[1].Reason.Error()}
	require.Contains(t, errorMsgs, "even number: 2")
	require.Contains(t, errorMsgs, "even number: 4")
}

func TestChunk_PanicInvalidChunkSize(t *testing.T) {
	src := pipefn.From(seqOf(1, 2, 3))

	require.Panics(t, func() {
		pipefn.Chunk(src, -1)
	})

	require.Panics(t, func() {
		pipefn.Chunk(src, 0)
	})

}

func TestChunk_GroupsCorrectly(t *testing.T) {
	src := pipefn.From(seqOf(1, 2, 3, 4, 5))

	p := pipefn.Chunk(src, 2)

	vals, errs := collect(p)

	require.Equal(t, [][]int{
		{1, 2},
		{3, 4},
		{5},
	}, vals)

	require.Empty(t, errs)
}

func TestFlatMap_FlattensInOrder(t *testing.T) {
	src := pipefn.From(seqOf(1, 2, 3))

	p := pipefn.FlatMap(src, func(v int) []int {
		return []int{v, v * 10}
	})

	vals, errs := collect(p)

	require.Equal(t, []int{
		1, 10,
		2, 20,
		3, 30,
	}, vals)

	require.Empty(t, errs)
}

func TestMerge(t *testing.T) {
	p1 := pipefn.From(seqOf(1, 2))
	p2 := pipefn.From(seqOf(3, 4))

	merged := pipefn.Merge(p1, p2)

	vals, errs := collect(merged)

	require.ElementsMatch(t, []int{1, 2, 3, 4}, vals)
	require.Empty(t, errs)
}

func TestMerge_ForwardsErrors(t *testing.T) {
	// Pipe 1: values 1, 2, emits an error for 2
	pipe1 := pipefn.From(seqOf(1, 2))
	pipe1 = pipefn.TryMap(pipe1, func(v int) (int, error) {
		if v == 2 {
			return 0, fmt.Errorf("pipe1 error")
		}
		return v, nil
	})

	// Pipe 2: values 3, 4, emits an error for 4
	pipe2 := pipefn.From(seqOf(3, 4))
	pipe2 = pipefn.TryMap(pipe2, func(v int) (int, error) {
		if v == 4 {
			return 0, fmt.Errorf("pipe2 error")
		}
		return v, nil
	})

	merged := pipefn.Merge(pipe1, pipe2)

	vals, errs := collect(merged)

	// All successful values should be present
	require.ElementsMatch(t, []int{1, 3}, vals)

	// Errors from both pipes should be reported
	require.Len(t, errs, 2)
	errorMsgs := []string{errs[0].Reason.Error(), errs[1].Reason.Error()}
	require.Contains(t, errorMsgs, "pipe1 error")
	require.Contains(t, errorMsgs, "pipe2 error")
}

func TestFlatTryMap(t *testing.T) {
	// tests that FlatTryMap behaves identically to Flatten(TryMap)

	p1 := pipefn.From(seqOf("A,B,C", "D,E,F"))
	v1 := pipefn.FlatTryMap(p1, func(in string) ([]string, error) {
		return strings.Split(in, ","), nil
	})

	p2 := pipefn.From(seqOf("A,B,C", "D,E,F"))
	v2 := pipefn.Flatten(pipefn.TryMap(p2, func(in string) ([]string, error) { return strings.Split(in, ","), nil }))

	values1, errs1 := collect(v1)
	values2, errs2 := collect(v2)

	require.Equal(t, values1, values2)
	require.Equal(t, errs1, errs2)
}

func TestGroupBy(t *testing.T) {
	// Define a simple pipe of letters
	input := pipefn.From(seqOf("A", "A", "B", "B", "A", "C", "C", "C"))

	// Group consecutive letters
	grouped := pipefn.GroupBy(input, func(s string) string { return s })

	vals, errs := collect(grouped)

	// No errors expected
	require.Empty(t, errs)

	// Expected consecutive grouping
	expected := [][]string{
		{"A", "A"},
		{"B", "B"},
		{"A"},
		{"C", "C", "C"},
	}
	require.Equal(t, expected, vals)
}

func TestGroupBy_ForwardsErrors(t *testing.T) {
	// Input values with a TryMap stage to inject errors
	input := pipefn.From(seqOf(1, 1, 2, 2, 3))
	inputWithErr := pipefn.TryMap(input, func(v int) (int, error) {
		if v == 2 {
			return 0, fmt.Errorf("bad value %d", v)
		}
		return v, nil
	})

	// Group by value
	grouped := pipefn.GroupBy(inputWithErr, func(v int) int { return v })

	vals, errs := collect(grouped)

	// The values emitted should skip the elements that caused errors (2)
	expectedVals := [][]int{
		{1, 1},
		{3},
	}
	require.Equal(t, expectedVals, vals)

	// Errors should include the one produced for value 2
	require.Len(t, errs, 2)
	for _, err := range errs {
		require.Contains(t, err.Error(), "bad value 2")
	}
}

func TestGroupByAggregate_SumPerGroup(t *testing.T) {
	type Record struct {
		Key   string
		Value int
	}

	input := pipefn.From(seqOf(
		Record{"A", 1},
		Record{"A", 2},
		Record{"B", 10},
		Record{"B", 5},
		Record{"A", 3}, // new A group
	))

	aggregated := pipefn.GroupByAggregate(input,
		func(r Record) string { return r.Key },       // key function
		func(first Record) int { return 0 },          // init accumulator
		func(acc *int, r Record) { *acc += r.Value }) // update accumulator

	vals, errs := collect(aggregated)

	require.Empty(t, errs)
	require.Equal(t, []int{3, 15, 3}, vals) // A1+A2=3, B1+B2=15, A3=3
}

func TestGroupByAggregate_EmptyInput(t *testing.T) {
	type Record struct {
		Key   string
		Value int
	}
	input := pipefn.From(seqOf[Record]())
	aggregated := pipefn.GroupByAggregate(input,
		func(r Record) string { return r.Key },
		func(first Record) int { return 0 },
		func(acc *int, r Record) { *acc += r.Value })

	vals, errs := collect(aggregated)
	require.Empty(t, vals)
	require.Empty(t, errs)
}

func TestGroupByAggregate_PreservesErrors(t *testing.T) {
	type Record struct {
		Key   string
		Value int
	}
	input := pipefn.From(seqOf(
		Record{"A", 1},
		Record{"A", 2},
		Record{"B", 10},
	))
	inputWithErr := pipefn.TryMap(input, func(r Record) (Record, error) {
		if r.Value == 2 {
			return Record{}, fmt.Errorf("bad value %d", r.Value)
		}
		return r, nil
	})

	aggregated := pipefn.GroupByAggregate(inputWithErr,
		func(r Record) string { return r.Key },
		func(first Record) int { return 0 },
		func(acc *int, r Record) { *acc += r.Value })

	vals, errs := collect(aggregated)

	// The valid values are aggregated, ignoring the ones that produced errors
	require.Equal(t, []int{1, 10}, vals)

	// The error for Value=2 is preserved
	require.Len(t, errs, 1)
	require.EqualError(t, errs[0].Reason, "bad value 2")
}

func seqOf[T any](values ...T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, v := range values {
			if !yield(v) {
				return
			}
		}
	}
}
func collect[T any](p pipefn.Pipe[T]) ([]T, []pipefn.PipelineError) {
	valsCh, errsCh := p.Results()

	var vals []T
	var errs []pipefn.PipelineError

	done := make(chan struct{})

	go func() {
		for err := range errsCh {
			errs = append(errs, err)
		}
		close(done)
	}()

	for v := range valsCh {
		vals = append(vals, v)
	}

	<-done
	return vals, errs
}
