package pipefn_test

import (
	"errors"
	"fmt"
	"iter"
	"testing"
	"time"

	"github.com/KasperOmsK/pipefn"
	"github.com/stretchr/testify/require"
)

// Scenario1to1 is a helper that run tests for each
// invariant that every 1-to-1 transformation should have.
// Namely:
// Empty in => empty out
// Lazy: input is not iterated before the first iteration loop
// Backpressure: Breaking out of iteration stops reading from input and does not block
// Error propagation: input internal errors are forwarded to output internal errors
// Failure propagation: input terminal failure guarantees output terminal failure
type Scenario1to1[In, Out any] struct {
	inputFactory DataFactory[In]
	apply        func(input pipefn.Pipe[In]) pipefn.Pipe[Out]
}

type DataFactory[T any] interface {
	Random(n int) []T
}

func NewUnaryScenario[In, Out any](inputFactory DataFactory[In], apply func(input pipefn.Pipe[In]) pipefn.Pipe[Out]) *Scenario1to1[In, Out] {
	return &Scenario1to1[In, Out]{
		inputFactory: inputFactory,
		apply:        apply,
	}
}

// absolute invariants (common to all transformations):
func (us *Scenario1to1[In, Out]) Run(t *testing.T) {

	t.Run("empty in empty out", func(t *testing.T) {
		// empty input should always yield empty output
		// FIXME: actually thats not true: counter example: DefaultIfEmpty

		empty := []In{}
		out := us.apply(pipefn.FromSlice(empty))
		requireEmpty(t, out)
	})

	t.Run("is lazy", func(t *testing.T) {
		// upstream should be opened only after consuming
		inputStream := newSpyStream(seqStream[In]{
			seq: func(yield func(In) bool) {
				// Yield at least one value to make sure
				// we actually starts the input iterator
				yield(*new(In))
			},
		})

		out := us.apply(pipefn.From(inputStream))

		// apply only creates the output pipe, so wasOpened should be false
		require.False(t, inputStream.wasOpened)

		values := out.Values()

		// still lazy after results called
		require.False(t, inputStream.wasOpened)

		for range values.Seq() {
		}

		require.True(t, inputStream.wasOpened)
		require.True(t, inputStream.ended)
	})

	t.Run("early break", func(t *testing.T) {

		// breaking early should not block and stop reading from upstream
		//
		// This may be tricky to test since this is "best effort". Some transformation will read more value from upstream
		// than are ultimately produced (e.g. GroupBy, Chunk)

		fakeData := us.inputFactory.Random(1000)
		input := pipefn.FromSlice(fakeData)

		out := us.apply(input)

		// Since I've regularly created deadlocks during my attempts at implementing various
		// transformations, I think it's at least relevent to test that consuming does not block
		// indefinitely.
		requireCompletesIn(t, func() {
			for range out.Values().Seq() {
				break
			}
		}, time.Second)

	})

	t.Run("input errors propagate", func(t *testing.T) {
		// Every upstream error must be observable downstream exactly once and in order

		inputData := us.inputFactory.Random(50)

		expectedErrs := make([]error, 0)

		errProducer := func(i int, v In) (In, error) {
			if i%5 == 0 {
				err := fmt.Errorf("err-%d", i)
				expectedErrs = append(expectedErrs, err)
				var zero In
				return zero, err
			}
			return v, nil
		}

		var idx int
		upstream := pipefn.TryMap(
			pipefn.FromSlice(inputData),
			func(v In) (In, error) {
				res, err := errProducer(idx, v)
				idx++
				return res, err
			},
		)

		out := us.apply(upstream)

		values, errs := out.Results()

		go func() {
			for range values.Seq() {
			}
		}()

		var got []error
		for e := range errs {
			got = append(got, e)
		}

		// there need to be at least as much errors as we generated
		require.GreaterOrEqual(t, len(got), len(expectedErrs))

		// errors should all appear in order
		i := 0
		remaining := len(expectedErrs)
		for remaining > 0 && i < len(got) {
			currentErr := got[i]
			expectedErr := expectedErrs[len(expectedErrs)-remaining]

			if errors.Is(currentErr, expectedErr) {
				remaining--
			}
			i++
		}

		require.Zero(t, remaining, "missing errors: some errors were not found or they were not emitted in order")
	})

	t.Run("input failure propagates", func(t *testing.T) {
		// a failure *encountered* in an upstream pipe should always return a pipe that fails
		//
		// Since this a unary transformation, I suppose the condition is even stricter:
		// 	The output pipe should fail with an error that Is (as in errors.Is()) the input error
		//
		// This is not as straightforward for ManyToOne (and OneToMany ?) transformations, e.g. Merge should fail with the
		// *first* failure encountered.

		// NOTE: careful with the implementation of this test, note the emphasis on *encountered* failure.
		// Some transformations may (by design) stop before the upstream ends.
		// For example
		// Take(somePipe, 5) => if somePipe fails *after* 5 items, then the iteration will
		// never reach the failure and thus the output shouldn't fail.
		// The test should take this into consideration and only expect a failure if the input
		// *actually* ended with an error.

		failure := fmt.Errorf("ouch")
		failingPipe := pipefn.From(errorStream[In](failure))
		p := us.apply(failingPipe)

		_, _, err := collect(p)

		require.ErrorIs(t, err, failure)
	})
}

type spyStream[T any] struct {
	upstream pipefn.Stream[T]

	wasOpened bool
	ended     bool
	errCalled bool
}

func newSpyStream[T any](upstream pipefn.Stream[T]) *spyStream[T] {
	return &spyStream[T]{
		upstream: upstream,
	}
}

func (ss *spyStream[T]) Seq() iter.Seq[T] {
	return func(yield func(T) bool) {
		defer func() {
			ss.ended = true
		}()
		for i := range ss.upstream.Seq() {
			ss.wasOpened = true
			if !yield(i) {
				return
			}
		}
	}
}

func (ss *spyStream[T]) Err() error {
	ss.errCalled = true
	return ss.upstream.Err()
}
