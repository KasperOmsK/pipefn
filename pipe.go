package pipefn

import (
	"errors"
	"fmt"
	"iter"
	"sync"
)

var (
	ErrAlreadyConsumed = errors.New("pipefn: pipe has already been consumed")
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
	//
	// The zero Pipe is an empty pipe that yields no value and no error.
	Pipe[T any] struct {
		*header
		values Stream[T]
	}

	// PipeError represents an error that occured inside a pipeline
	PipeError struct {
		// The item that generated the error
		Item any
		// The actual error
		Reason error
	}

	header struct {
		used   bool
		mu     sync.Mutex
		errors chan error
	}
)

// From creates a pipe that produces values from the provided stream.
//
// The returned pipe will have the same terminal error as s, if any
func From[T any](s Stream[T]) Pipe[T] {
	if s == nil {
		panic("pipefn.From: stream is nil")
	}

	return Pipe[T]{
		header: &header{
			errors: make(chan error),
		},
		values: s,
	}
}

// FromSeq creates a Pipe that produces values from the provided iter.Seq.
//
// FromSeq panics if seq is nil.
func FromSeq[T any](seq iter.Seq[T]) Pipe[T] {
	if seq == nil {
		panic("pipefn.FromSeq: nil seq")
	}
	return From(seqStream[T]{
		seq: seq,
	})
}

// FromSlice creates a Pipe that produces the elements of elems in order.
//
// The resulting Pipe yields each element of the slice sequentially.
// The slice is not copied; iteration reads directly from elems.
//
// If elems is nil, the resulting Pipe behaves as an empty pipe.
func FromSlice[T any](elems []T) Pipe[T] {
	return FromSeq(func(yield func(T) bool) {
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
	return FromSeq(func(yield func(T) bool) {
		for c := range ch {
			if !yield(c) {
				return
			}
		}
	})
}

// Results returns a Stream that yields the values produced by p,
// along with a channel that emits errors produced by the pipeline.
//
// A Pipe can be consumed only once. After the first successful call to Results,
// any subsequent call returns an empty Stream whose Err() returns ErrAlreadyConsumed,
// along with a closed error channel.
//
// The errors channel contains only transformation errors produced by
// pipeline stages (for example via TryMap). Terminal failures of the
// underlying Stream are not sent through this channel and must instead
// be checked via values.Err() after iteration completes.
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
//	done := make(chan struct{})
//	go func() {
//	    defer close(done)
//	    for err := range errs {
//	        perr := err.(*PipeError)
//	        log.Printf("pipeline error: item %v: %s", perr.Item, perr.Reason)
//	    }
//	}()
//
//	// iterate over values
//	for v := range values.Seq() {
//	    process(v)
//	}
//
//	if err := values.Err() != nil {
//	    // handle terminal failure (e.g. rollback or discard processed work)
//	}
//
//	<-done
func (p Pipe[T]) Results() (values Stream[T], errs <-chan error) {
	if p.header == nil {
		empty := emptyPipe[T]()
		return empty.Results()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// if used, return a empty and "closed" stream
	if p.used {
		errCh := make(chan error)
		close(errCh)
		out := errorStream[T](ErrAlreadyConsumed)
		return out, errCh
	}

	p.used = true
	return seqStream[T]{
		seq: func(yield func(T) bool) {
			defer close(p.errors)
			for e := range p.values.Seq() {
				if !yield(e) {
					return
				}
			}
		},
		errFunc: p.values.Err,
	}, p.errors
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
//	for v := range stream.Seq() {
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
	done := make(chan struct{})
	go func() {
		for err := range errors {
			errorFn(err)
		}
		close(done)
	}()
	for i := range values.Seq() {
		consumeFn(i)
	}
	<-done
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
		outValues []T
		outErrors []error
	)

	values, errs := p.Results()

	done := make(chan struct{})
	go func() {
		for e := range errs {
			outErrors = append(outErrors, e)
		}
		close(done)
	}()

	for v := range values.Seq() {
		outValues = append(outValues, v)
	}
	<-done

	return outValues, outErrors, values.Err()
}

// Tap modifies p so that tapFn is called for each element that passes through p.
// The tap function is called before the element is yielded.
//
// Calling Tap multiple times will chain tap functions in the order they were added.
//
// Tap panics if tapFn is nil.
func (p *Pipe[T]) Tap(tapFn func(T)) {

	p.mu.Lock()
	defer p.mu.Unlock()

	if tapFn == nil {
		panic("Pipe.Tap: tapFn cannot be nil")
	}

	originalIt := p.values.Seq()
	p.values = seqStream[T]{
		seq: func(yield func(T) bool) {
			for i := range originalIt {
				tapFn(i)
				if !yield(i) {
					return
				}
			}
		},
	}
}

func (pe *PipeError) Error() string {
	return fmt.Sprintf("%+v: %s", pe.Item, pe.Reason)
}

func emptyPipe[T any]() Pipe[T] {
	return FromSlice([]T{})
}
