package pipefn_test

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KasperOmsK/pipefn"

	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {

	t.Run("test invariants", func(t *testing.T) {
		sc := NewUnaryScenario(IntFactory{}, func(input pipefn.Pipe[int]) pipefn.Pipe[int] {
			return pipefn.Map(input, func(in int) int { return in * in })
		})
		sc.Run(t)
	})

	t.Run("transforms correctly", func(t *testing.T) {

		src := pipefn.FromSeq(seqOf(1, 2, 3))

		p := pipefn.Map(src, func(v int) int {
			return v * 2
		})

		vals, errs, err := collect(p)
		require.NoError(t, err)

		require.Equal(t, []int{2, 4, 6}, vals)
		require.Empty(t, errs)
	})

	t.Run("panics if fn is nil", func(t *testing.T) {
		require.Panics(t, func() {
			pipefn.Map[int, int](pipefn.Pipe[int]{}, nil)
		})
	})
}

func TestTryMap(t *testing.T) {

	t.Run("test invariants", func(t *testing.T) {
		t.Run("without errors", func(t *testing.T) {
			sc := NewUnaryScenario(IntFactory{}, func(input pipefn.Pipe[int]) pipefn.Pipe[int] {
				return pipefn.TryMap(input, func(in int) (int, error) {
					return in * in, nil
				})
			})
			sc.Run(t)
		})
		t.Run("with errors", func(t *testing.T) {
			sc := NewUnaryScenario(IntFactory{}, func(input pipefn.Pipe[int]) pipefn.Pipe[int] {
				return pipefn.TryMap(input, func(in int) (int, error) {
					return 0, fmt.Errorf("!!!explosion noise!!!")
				})
			})
			sc.Run(t)
		})
	})

	t.Run("generates errors", func(t *testing.T) {
		src := pipefn.FromSeq(seqOf(1, 2, 3, 4))

		p := pipefn.TryMap(src, func(v int) (int, error) {
			if v%2 == 0 {
				return 0, fmt.Errorf("even number: %d", v)
			}
			return v * 10, nil
		})

		vals, errs, err := collect(p)
		require.NoError(t, err)

		require.Equal(t, []int{10, 30}, vals)
		require.Len(t, errs, 2)
	})

	t.Run("panics if fn is nil", func(t *testing.T) {
		require.Panics(t, func() {
			pipefn.TryMap[int, int](pipefn.Pipe[int]{}, nil)
		})
	})
}

func TestFilter(t *testing.T) {

	t.Run("test invariants", func(t *testing.T) {
		sc := NewUnaryScenario(IntFactory{}, func(input pipefn.Pipe[int]) pipefn.Pipe[int] {
			return pipefn.Filter(input, func(item int) bool { return item%2 == 0 })
		})
		sc.Run(t)
	})

	t.Run("filters correctly", func(t *testing.T) {
		src := pipefn.FromSeq(seqOf(1, 2, 3, 4, 5))

		p := pipefn.Filter(src, func(v int) bool {
			return v%2 == 0
		})

		vals, errs, err := collect(p)
		require.NoError(t, err)

		require.Equal(t, []int{2, 4}, vals)
		require.Empty(t, errs)
	})

	t.Run("panics if predicate is nil", func(t *testing.T) {
		require.Panics(t, func() {
			pipefn.Filter(pipefn.Pipe[int]{}, nil)
		})
	})
}

func TestChunk(t *testing.T) {

	t.Run("test invariants", func(t *testing.T) {
		sc := NewUnaryScenario(IntFactory{}, func(input pipefn.Pipe[int]) pipefn.Pipe[[]int] {
			return pipefn.Chunk(input, 10)
		})
		sc.Run(t)
	})

	t.Run("chunks correctly", func(t *testing.T) {
		src := pipefn.FromSeq(seqOf(1, 2, 3, 4, 5))

		p := pipefn.Chunk(src, 2)

		vals, errs, err := collect(p)
		require.NoError(t, err)

		require.Equal(t, [][]int{
			{1, 2},
			{3, 4},
			{5},
		}, vals)

		require.Empty(t, errs)
	})

	t.Run("panics if invalid chunk size", func(t *testing.T) {
		src := pipefn.FromSeq(seqOf(1, 2, 3))

		require.Panics(t, func() {
			pipefn.Chunk(src, -1)
		})

		require.Panics(t, func() {
			pipefn.Chunk(src, 0)
		})

	})
}

func TestMerge(t *testing.T) {

	t.Run("merge correctly", func(t *testing.T) {
		p1 := pipefn.FromSeq(seqOf(1, 2))
		p2 := pipefn.FromSeq(seqOf(3, 4))

		merged := pipefn.Merge(p1, p2)

		vals, errs, err := collect(merged)
		require.NoError(t, err)

		require.ElementsMatch(t, []int{1, 2, 3, 4}, vals)
		require.Empty(t, errs)
	})

	t.Run("forwards errors", func(t *testing.T) {
		// Pipe 1: values 1, 2, emits an error for 2
		pipe1 := pipefn.FromSeq(seqOf(1, 2))
		pipe1 = pipefn.TryMap(pipe1, func(v int) (int, error) {
			if v == 2 {
				return 0, fmt.Errorf("pipe1 error")
			}
			return v, nil
		})

		// Pipe 2: values 3, 4, emits an error for 4
		pipe2 := pipefn.FromSeq(seqOf(3, 4))
		pipe2 = pipefn.TryMap(pipe2, func(v int) (int, error) {
			if v == 4 {
				return 0, fmt.Errorf("pipe2 error")
			}
			return v, nil
		})

		merged := pipefn.Merge(pipe1, pipe2)

		vals, errs, err := collect(merged)
		require.NoError(t, err)

		// All successful values should be present
		require.ElementsMatch(t, []int{1, 3}, vals)

		// Errors from both pipes should be reported
		require.Len(t, errs, 2)
		errorMsgs := []string{toPipelineError(errs[0]).Reason.Error(), toPipelineError(errs[1]).Reason.Error()}
		require.Contains(t, errorMsgs, "pipe1 error")
		require.Contains(t, errorMsgs, "pipe2 error")
	})

	t.Run("abort early", func(t *testing.T) {
		p1 := pipefn.FromSeq(func(yield func(int) bool) {
			for {
				time.Sleep(time.Millisecond * 10)
				if !yield(1) {
					return
				}
			}
		})
		p2 := pipefn.From(errorStream[int](fmt.Errorf("stream error")))

		requireCompletesIn(t, func() {
			merged := pipefn.Merge(p1, p2)
			_, errs, err := collect(merged)
			require.Error(t, err)
			require.Empty(t, errs)
		}, time.Second)
	})

	t.Run("same lineage", func(t *testing.T) {
		p := pipefn.FromSlice([]int{1, 2, 3})
		p2 := pipefn.Map(p, func(in int) int { return 2 * in })
		_, _, err := collect(pipefn.Merge(p, p2))
		require.ErrorIs(t, err, pipefn.ErrAlreadyConsumed)
	})

	t.Run("no input", func(t *testing.T) {
		p := pipefn.Merge[int]()
		requireEmpty(t, p)
	})
}

func TestFlatTryMap(t *testing.T) {
	// tests that FlatTryMap behaves identically to Flatten(TryMap)

	t.Run("test invariants", func(t *testing.T) {

		t.Run("without errors", func(t *testing.T) {
			sc := NewUnaryScenario(IntFactory{}, func(input pipefn.Pipe[int]) pipefn.Pipe[int] {
				return pipefn.FlatTryMap(input, func(in int) ([]int, error) {
					return []int{1, 2, 3}, nil
				})
			})
			sc.Run(t)
		})

		t.Run("with errors", func(t *testing.T) {
			sc := NewUnaryScenario(IntFactory{}, func(input pipefn.Pipe[int]) pipefn.Pipe[int] {
				return pipefn.FlatTryMap(input, func(in int) ([]int, error) {
					return nil, fmt.Errorf("badaboom")
				})
			})
			sc.Run(t)
		})

	})

	t.Run("behaves correctly", func(t *testing.T) {
		tryMap := func(in string) ([]string, error) {

			if strings.Contains(in, "G") {
				return nil, errors.New("bad group")
			}

			return strings.Split(in, ","), nil
		}

		data := []string{"A,B,C", "D,E,F", "G,H,I"}
		p1 := pipefn.FromSeq(seqOf(data...))
		v1 := pipefn.FlatTryMap(p1, tryMap)

		p2 := pipefn.FromSeq(seqOf(data...))
		v2 := pipefn.Flatten(pipefn.TryMap(p2, tryMap))

		values1, errs1, err1 := collect(v1)
		values2, errs2, err2 := collect(v2)
		require.NoError(t, err1)
		require.NoError(t, err2)

		require.Equal(t, values1, values2)
		require.Equal(t, errs1, errs2)
	})

	t.Run("panics if fn is nil", func(t *testing.T) {
		require.Panics(t, func() {
			pipefn.FlatTryMap[int, []int](pipefn.Pipe[int]{}, nil)
		})
	})
}

func TestFlatMap(t *testing.T) {

	t.Run("test invariants", func(t *testing.T) {

		t.Run("without errors", func(t *testing.T) {
			sc := NewUnaryScenario(IntFactory{}, func(input pipefn.Pipe[int]) pipefn.Pipe[int] {
				return pipefn.FlatMap(input, func(in int) []int {
					return []int{
						in % 5,
						in - (in % 5),
					}
				})
			})
			sc.Run(t)
		})
	})

	t.Run("identical to Flatten(Map())", func(t *testing.T) {
		// tests that FlatMap behaves identically to Flatten(Map)

		groupFn := func(in string) []string {
			return strings.Split(in, ",")
		}
		p1 := pipefn.FromSeq(seqOf("A,B,C", "D,E,F"))
		v1 := pipefn.FlatMap(p1, groupFn)

		p2 := pipefn.FromSeq(seqOf("A,B,C", "D,E,F"))
		v2 := pipefn.Flatten(pipefn.Map(p2, groupFn))

		values1, errs1, err1 := collect(v1)
		values2, errs2, err2 := collect(v2)
		require.NoError(t, err1)
		require.NoError(t, err2)

		require.Equal(t, values1, values2)
		require.Equal(t, errs1, errs2)
	})

	t.Run("flattens in order", func(t *testing.T) {

		src := pipefn.FromSeq(seqOf(1, 2, 3))

		p := pipefn.FlatMap(src, func(v int) []int {
			return []int{v, v * 10}
		})

		vals, errs, err := collect(p)
		require.NoError(t, err)

		require.Equal(t, []int{
			1, 10,
			2, 20,
			3, 30,
		}, vals)

		require.Empty(t, errs)
	})

	t.Run("panics if fn is nil", func(t *testing.T) {
		require.Panics(t, func() {
			pipefn.FlatMap[int, []int](pipefn.Pipe[int]{}, nil)
		})
	})
}

func TestGroupByKey(t *testing.T) {

	t.Run("test invariants", func(t *testing.T) {
		sc := NewUnaryScenario(IntFactory{}, func(input pipefn.Pipe[int]) pipefn.Pipe[[]int] {
			return pipefn.GroupByKey(input, func(t int) int { return t / 10 })
		})
		sc.Run(t)
	})

	t.Run("group correctly", func(t *testing.T) {
		// Define a simple pipe of letters
		input := pipefn.FromSeq(seqOf("A", "A", "B", "B", "A", "C", "C", "C"))

		// Group consecutive letters
		grouped := pipefn.GroupByKey(input, func(s string) string { return s })

		vals, errs, err := collect(grouped)
		require.NoError(t, err)

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
	})

	t.Run("panics if keyFunc is nil", func(t *testing.T) {
		require.Panics(t, func() {
			pipefn.GroupByKey[int, int](pipefn.Pipe[int]{}, nil)
		})
	})
}

type Record struct {
	Key   string
	Value int
}
type RecordFactory struct{}

func (rf RecordFactory) Random(n int) []Record {
	out := []Record{}
	for i := range n {
		out = append(out, Record{
			Key:   strconv.Itoa(i),
			Value: rand.Intn(1000),
		})
	}
	return out
}

func TestGroupByAggregate(t *testing.T) {

	t.Run("test invariants", func(t *testing.T) {
		sc := NewUnaryScenario(RecordFactory{}, func(input pipefn.Pipe[Record]) pipefn.Pipe[int] {
			return pipefn.GroupByKeyAggregate(input,
				func(r Record) string { return r.Key },       // key function
				func(first Record) int { return 0 },          // init accumulator
				func(acc *int, r Record) { *acc += r.Value }) // update accumulator
		})
		sc.Run(t)
	})

	t.Run("sum per group", func(t *testing.T) {
		input := pipefn.FromSeq(seqOf(
			Record{"A", 1},
			Record{"A", 2},
			Record{"B", 10},
			Record{"B", 5},
			Record{"A", 3}, // new A group
		))

		aggregated := pipefn.GroupByKeyAggregate(input,
			func(r Record) string { return r.Key },       // key function
			func(first Record) int { return 0 },          // init accumulator
			func(acc *int, r Record) { *acc += r.Value }) // update accumulator

		vals, errs, err := collect(aggregated)
		require.NoError(t, err)

		require.Empty(t, errs)
		require.Equal(t, []int{3, 15, 3}, vals) // A1+A2=3, B1+B2=15, A3=3
	})

	t.Run("panics if keyFunc is nil", func(t *testing.T) {
		require.Panics(t, func() {
			pipefn.GroupByKeyAggregate[int, int, int](pipefn.Pipe[int]{}, nil, func(first int) int { return 0 }, func(acc *int, item int) {})
		})
	})

	t.Run("panics if initFunc is nil", func(t *testing.T) {
		require.Panics(t, func() {
			pipefn.GroupByKeyAggregate[int, int, int](pipefn.Pipe[int]{}, func(i int) int { return 0 }, nil, func(acc *int, item int) {})
		})
	})

	t.Run("panics if updateFunc is nil", func(t *testing.T) {
		require.Panics(t, func() {
			pipefn.GroupByKeyAggregate[int, int, int](pipefn.Pipe[int]{}, func(i int) int { return 0 }, func(first int) int { return 0 }, nil)
		})
	})
}

func TestConcat(t *testing.T) {
	t.Run("concatenates values in order", func(t *testing.T) {
		p1 := pipefn.FromSlice([]int{1, 2})
		p2 := pipefn.FromSlice([]int{3, 4})

		values, errs, failure := collect(pipefn.Concat(p1, p2))

		require.Equal(t, []int{1, 2, 3, 4}, values)
		require.Empty(t, errs)
		require.NoError(t, failure)
	})

	t.Run("handles empty pipe in sequence", func(t *testing.T) {
		p1 := pipefn.FromSlice([]int{1, 2})
		p2 := pipefn.FromSlice([]int(nil))
		p3 := pipefn.FromSlice([]int{3})

		values, errs, failure := collect(pipefn.Concat(p1, p2, p3))

		require.Equal(t, []int{1, 2, 3}, values)
		require.Empty(t, errs)
		require.NoError(t, failure)
	})

	t.Run("all pipes empty", func(t *testing.T) {
		p1 := pipefn.FromSlice([]int(nil))
		p2 := pipefn.FromSlice([]int(nil))

		values, errs, failure := collect(pipefn.Concat(p1, p2))

		require.Empty(t, values)
		require.Empty(t, errs)
		require.NoError(t, failure)
	})

	t.Run("zero pipes", func(t *testing.T) {
		values, errs, failure := collect(pipefn.Concat[int]())

		require.Empty(t, values)
		require.Empty(t, errs)
		require.NoError(t, failure)
	})

	t.Run("with errors", func(t *testing.T) {
		mapErr := errors.New("map failure")

		p1 := pipefn.TryMap(
			pipefn.FromSlice([]int{1, 2, 3}),
			func(v int) (int, error) {
				if v == 2 {
					return 0, mapErr
				}
				return v, nil
			},
		)

		p2 := pipefn.TryMap(
			pipefn.FromSlice([]int{4, 5}),
			func(v int) (int, error) {
				if v == 5 {
					return 0, mapErr
				}
				return v, nil
			},
		)

		values, errs, failure := collect(pipefn.Concat(p1, p2))

		require.Equal(t, []int{1, 3, 4}, values)
		require.Len(t, errs, 2)
		require.NoError(t, failure)
	})

	t.Run("with terminal failure", func(t *testing.T) {
		terminalErr := errors.New("terminal failure")

		p1 := pipefn.FromSlice([]int{1, 2})
		p2 := pipefn.From(errorStream[int](terminalErr))

		values, errs, failure := collect(pipefn.Concat(p1, p2))

		// Values from the first pipe should still be emitted
		require.Equal(t, []int{1, 2}, values)

		// No intermediate pipe errors expected
		require.Empty(t, errs)

		// Terminal failure should propagate
		require.ErrorIs(t, failure, terminalErr)
	})

	t.Run("with terminal failure on first pipe", func(t *testing.T) {
		terminalErr := errors.New("terminal failure")

		p1 := pipefn.From(errorStream[int](terminalErr))
		p2 := pipefn.FromSlice([]int{3, 4})

		values, errs, failure := collect(pipefn.Concat(p1, p2))

		require.Empty(t, values)
		require.Empty(t, errs)
		require.ErrorIs(t, failure, terminalErr)
	})
}

func TestZip(t *testing.T) {

	lSlice := []string{"a", "b", "c"}
	rSlice := []int{1, 2, 3}

	t.Run("zips correctly", func(t *testing.T) {

		left := pipefn.FromSlice(lSlice)
		right := pipefn.FromSlice(rSlice)
		zipped := pipefn.Zip(left, right)

		values, errs, err := collect(zipped)

		require.Empty(t, errs)
		require.NoError(t, err)
		require.Len(t, values, len(rSlice))

		for i, v := range values {
			require.Equal(t, lSlice[i], v.LValue)
			require.Equal(t, rSlice[i], v.RValue)
		}
	})

	t.Run("break not blocks", func(t *testing.T) {
		left := pipefn.FromSlice(lSlice)
		right := pipefn.FromSlice(rSlice)
		zipped := pipefn.Zip(left, right)

		values := zipped.Values()

		requireCompletesIn(t, func() {
			for range values.Seq() {
				break
			}
		}, time.Second)

		require.NoError(t, values.Err())
	})

	t.Run("stops on shorter left", func(t *testing.T) {
		longer := pipefn.FromSlice(lSlice)
		shorter := pipefn.FromSlice([]int{1})
		zipped := pipefn.Zip(shorter, longer)

		values, errs, err := collect(zipped)

		require.Empty(t, errs)
		require.NoError(t, err)
		require.Len(t, values, 1)
	})

	t.Run("stops on shorter right", func(t *testing.T) {
		longer := pipefn.FromSlice(lSlice)
		shorter := pipefn.FromSlice([]int{1})
		zipped := pipefn.Zip(longer, shorter)

		values, errs, err := collect(zipped)

		require.Empty(t, errs)
		require.NoError(t, err)
		require.Len(t, values, 1)

	})

	t.Run("forwards errors from left", func(t *testing.T) {
		errorPipe := pipeWithError[int](len(rSlice))

		valuePipe := pipefn.FromSlice(rSlice)
		zipped := pipefn.Zip(errorPipe, valuePipe)
		values, errs, err := collect(zipped)

		require.Len(t, values, 0)
		require.Len(t, errs, len(rSlice))
		require.NoError(t, err)
	})

	t.Run("forwards errors from right", func(t *testing.T) {
		errorPipe := pipeWithError[int](len(rSlice))
		valuePipe := pipefn.FromSlice(rSlice)
		zipped := pipefn.Zip(valuePipe, errorPipe)
		values, errs, err := collect(zipped)

		require.Len(t, values, 0)
		require.Len(t, errs, len(rSlice))
		require.NoError(t, err)
	})

	t.Run("left terminal failure", func(t *testing.T) {
		okPipe := pipefn.FromSeq(func(yield func(int) bool) {
			for {
				time.Sleep(time.Millisecond * 10)
				if !yield(1) {
					return
				}
			}
		})
		terminalFailure := fmt.Errorf("stream error")
		failingPipe := pipefn.From(errorStream[int](terminalFailure))

		zipped := pipefn.Zip(failingPipe, okPipe)
		values, errs, err := collect(zipped)

		require.ErrorIs(t, err, terminalFailure)
		require.Empty(t, values)
		require.Empty(t, errs)

	})

	t.Run("right terminal failure", func(t *testing.T) {
		okPipe := pipefn.FromSeq(func(yield func(int) bool) {
			for {
				time.Sleep(time.Millisecond * 10)
				if !yield(1) {
					return
				}
			}
		})
		terminalFailure := fmt.Errorf("stream error")
		failingPipe := pipefn.From(errorStream[int](terminalFailure))

		zipped := pipefn.Zip(okPipe, failingPipe)
		values, errs, err := collect(zipped)

		require.ErrorIs(t, err, terminalFailure)
		require.Empty(t, values)
		require.Empty(t, errs)
	})
}

func pipeWithError[T any](errCount int) pipefn.Pipe[T] {
	fakeData := make([]int, errCount)
	for i := range errCount {
		fakeData[i] = i
	}
	p := pipefn.FromSlice(fakeData)
	stubValue := *new(T)
	return pipefn.TryMap(p, func(in int) (T, error) {
		return stubValue, fmt.Errorf("err %d", in)
	})
}

type IntFactory struct{}

func (IntFactory) Random(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = rand.Intn(math.MaxInt)
	}
	return out
}

type seqStream[T any] struct {
	seq     iter.Seq[T]
	errFunc func() error
}

func (ss seqStream[T]) Seq() iter.Seq[T] {
	if ss.seq == nil {
		return func(yield func(T) bool) {}
	}
	return ss.seq
}

func (ss seqStream[T]) Err() error {
	if ss.errFunc != nil {
		return ss.errFunc()
	}
	return nil
}

func errorStream[T any](eofErr error) pipefn.Stream[T] {
	return seqStream[T]{
		seq: func(yield func(T) bool) {},
		errFunc: func() error {
			return eofErr
		},
	}
}

func toPipelineError(err error) *pipefn.PipeError {
	return err.(*pipefn.PipeError)
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

func collect[T any](p pipefn.Pipe[T]) ([]T, []error, error) {
	values, errors := p.Results()

	var vals []T
	var errs []error

	done := make(chan struct{})

	go func() {
		for err := range errors {
			errs = append(errs, err)
		}
		close(done)
	}()

	for v := range values.Seq() {
		vals = append(vals, v)
	}

	<-done
	return vals, errs, values.Err()
}

func requireEmpty[T any](t *testing.T, p pipefn.Pipe[T]) {
	values, errs, err := collect(p)
	require.NoError(t, err, "unexpected error")
	require.Empty(t, values, "unexpected values")
	require.Empty(t, errs, "unexpected pipeline errors")
}

func requireCompletesIn(t *testing.T, fn func(), timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()

	select {
	case <-time.After(timeout):
		t.Errorf("function did not complete before %d", timeout)
	case <-done:
	}
}
