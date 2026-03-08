package pipefn

import (
	"fmt"
	"iter"
	"sync"
)

type (
	// Pipe represents a lazily-evaluated sequence of values of type T.
	//
	// Each transformation (Map, TryMap, Flatten etc.) produces a new Pipe that wraps the previous one.
	//
	// Errors produced by fallible stages (for example TryMap) are sent on the
	// pipeline's error channel. These errors describe failures associated with
	// individual items flowing through the pipeline.
	//
	// A pipe may also terminate with a final failure. This terminal failure
	// is reported by the value stream's Err() method once iteration completes.
	//
	// A Pipe is consumed when its Stream[T] is iterated, typically through Results,
	// Values, ForEach, Collect etc. Once consumed, a Pipe cannot be reused.
	// Refer to each consuming method's documentation for details on their behavior.
	Pipe[T any] struct {
		values Stream[T]
		errors chan error
	}

	// PipeError represents an error that occured inside a pipeline
	PipeError struct {
		// The item that generated the error
		Item any
		// The actual error
		Reason error
	}

	// Cursor provides pull-based iteration over a sequence of values.
	//
	// Next advances the cursor to the next value and reports whether one
	// is available. When Next returns false, iteration has finished or an
	// error has occurred.
	//
	// After Next returns true, Value returns the current value.
	//
	// Once iteration stops, Err reports any terminal error that occurred
	// during iteration. If the sequence completed successfully, Err returns nil.
	Cursor[T any] interface {
		Next() bool
		Value() T
		Err() error
	}
)

// From creates a Pipe that produces values from the provided iter.Seq.
//
// From panics if seq is nil.
func From[T any](seq iter.Seq[T]) Pipe[T] {

	if seq == nil {
		panic("pipefn.From: nil seq")
	}

	p := Pipe[T]{
		errors: make(chan error),
		values: Stream[T]{},
	}

	p.values.Seq = func(yield func(T) bool) {
		for item := range seq {
			if !yield(item) {
				return
			}
		}
	}

	return p
}

// FromCursor creates a Pipe that yields values from the provided Cursor.
//
// The returned Pipe iterates over cursor using Next and forwards each value
// returned by Value into the pipeline until the cursor is exhausted or the
// consumer stops iteration early.
//
// Any terminal error reported by Cursor.Err is exposed through the pipe's value stream
// Err method.
func FromCursor[T any](cursor Cursor[T]) Pipe[T] {
	if cursor == nil {
		panic("FromCursor: cursor is nil")
	}

	p := Pipe[T]{
		errors: make(chan error),
		values: Stream[T]{
			errFunc: cursor.Err,
		},
	}

	p.values.Seq = func(yield func(T) bool) {
		for cursor.Next() {
			if !yield(cursor.Value()) {
				return
			}
		}
	}

	return p
}

// FromSlice creates a Pipe that produces the elements of elems in order.
//
// The resulting Pipe yields each element of the slice sequentially.
// The slice is not copied; iteration reads directly from elems.
//
// If elems is nil, the resulting Pipe behaves as an empty pipe.
func FromSlice[T any](elems []T) Pipe[T] {
	return From(func(yield func(T) bool) {
		for _, e := range elems {
			if !yield(e) {
				return
			}
		}
	})
}

// FromChan creates a Pipe that produces values received from ch.
//
// The resulting Pipe yields each value read from the channel until the
// channel is closed. If the downstream consumer stops iteration early
// (i.e., yield returns false), iteration stops immediately and remaining
// values in the channel are ignored.
//
// FromChan does not close the provided channel; channel lifecycle remains
// the responsibility of the caller.
func FromChan[T any](ch <-chan T) Pipe[T] {
	return From(func(yield func(T) bool) {
		for c := range ch {
			if !yield(c) {
				return
			}
		}
	})
}

// Empty creates a Pipe that produces no values.
//
// The returned Pipe completes immediately when iterated and
// yields no elements and no errors.
func Empty[T any]() Pipe[T] {
	return From(func(yield func(T) bool) {})
}

// Results returns a Stream that yields the values produced by p,
// along with a channel that emits the errors produced by the p.
//
// Iterating over values.Seq consumes p. Once iteration begins,
// p cannot be reused, restarted, or iterated again.
//
// Callers must drain the errors channel. If errors are not consumed,
// stages in the pipeline that attempt to emit errors may block.
//
// Callers that wish to consume the pipeline without handling errors
// may use p.Values() instead.
//
// All errors emitted by the errors channel will be of type *PipeError.
//
// Typical usage:
//
//	values, errs := p.Results()
//
//	// drain errs
//	var wg sync.WaitGroup
//	wg.Add(1)
//	go func() {
//	    defer wg.Done()
//	    for err := range errs {
//	        perr := err.(*PipeError)
//	        log.Printf("pipeline error: item %v: %s", perr.Item, perr.Reason)
//	    }
//	}()
//
//	// iterate over values
//	for v := range values.Seq {
//	    process(v)
//	}
//
//	if err := values.Err() != nil {
//		// handle err (most likely rollback or discard the work done by process)
//	}
//
//	wg.Wait()
//	// at this point, both values and errs have been fully consumed.
func (p Pipe[T]) Results() (values Stream[T], errors <-chan error) {
	terminalStream := Stream[T]{
		errFunc: p.values.errFunc,
		Seq: func(yield func(T) bool) {
			defer close(p.errors)
			for i := range p.values.Seq {
				if !yield(i) {
					return
				}
			}
		},
	}
	return terminalStream, p.errors
}

// Values returns the sequence of values produced by p.
//
// Values behaves like Results, except that the error sequence is consumed
// internally and discarded. This allows callers who do not care about errors
// to ignore them while still ensuring the pipe completes correctly.
//
// Iterating over the returned values sequence consumes p. Once iteration
// begins, p cannot be reused or iterated again.
//
// Typical usage:
//
//	stream := p.Values()
//	for v := range stream.Seq {
//		process(v)
//	}
//
//	if err := stream.Err(); err != nil{
//		// handle err (most likely rollback or discard the work done by process)
//	}
func (p Pipe[T]) Values() Stream[T] {
	out, errs := p.Results()
	go func() {
		for range errs {
		}
	}()
	return out
}

// ForEach consumes all values from the Pipe, invoking consumeFn for each.
//
// Errors produced by the pipe are forwarded to errorFn.
//
// consumeFn and errorFn are executed concurrently. If shared state is accessed,
// the caller is responsible for ensuring proper synchronization.
//
// ForEach blocks until the pipe has been fully consumed and all pipeline
// errors have been delivered to errorFn. It then returns the terminal failure
// reported by the value stream, if any.
func (p Pipe[T]) ForEach(consumeFn func(item T), errorFn func(err error)) error {
	values, errors := p.Results()
	var errorWg sync.WaitGroup
	errorWg.Add(1)
	go func() {
		defer errorWg.Done()
		for err := range errors {
			errorFn(err)
		}
	}()
	for i := range values.Seq {
		consumeFn(i)
	}
	errorWg.Wait()

	return values.Err()
}

// Collect consumes the entire Pipe and returns all emitted values,
// all pipeline errors, and the terminal failure of the value stream.
//
// Values are collected in the order they are produced by the Pipe.
// Errors are collected in the order they are emitted.
//
// Collect fully drains the Pipe; after calling it, the Pipe cannot be consumed again.
func (p Pipe[T]) Collect() ([]T, []error, error) {
	var (
		values []T
		errors []error
	)

	failure := p.ForEach(func(item T) {
		values = append(values, item)
	}, func(err error) {
		errors = append(errors, err)
	})

	return values, errors, failure
}

// Tap modifies p so that tapFn is called for each element that passes through p.
// The tap function is called before the element is yielded.
//
// Calling Tap multiple times will chain tap functions in the order they were added.
//
// Tap panics if tapFn is nil.
func (p *Pipe[T]) Tap(tapFn func(T)) {

	if tapFn == nil {
		panic("Pipe.Tap: tapFn cannot be nil")
	}

	originalIt := p.values.Seq
	p.values.Seq = func(yield func(T) bool) {
		for i := range originalIt {
			tapFn(i)
			if !yield(i) {
				return
			}
		}
	}
}

func (pe *PipeError) Error() string {
	return fmt.Sprintf("%+v: %s", pe.Item, pe.Reason)
}
