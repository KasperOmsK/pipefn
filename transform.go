package pipefn

import (
	"context"
	"iter"
	"sync"
)

type (

	// MapFunc is a pure mapping function used by Map that transforms a value
	// of type In into a value of type Out.
	MapFunc[In, Out any] func(in In) Out

	// TryMapFunc is a mapping function that may return an error.
	//
	// Errors are added to the Pipe's error channel while
	// successful values continue through the pipeline.
	TryMapFunc[In, Out any] func(in In) (Out, error)

	// Predicate represents a filtering function that returns true when the
	// provided value should be included in the output stream.
	Predicate[T any] func(item T) bool
)

// TODO: consider making makeChildPipe public as a "low-level" primitive.
// This would allow users to make their own arbitrary transformations.
func makeChildPipe[In, Out any](
	parent Pipe[In],
	transform func(input iter.Seq[In], errs chan<- error) iter.Seq[Out]) Pipe[Out] {

	if parent.header == nil {
		return emptyPipe[Out]()
	}

	parentValues := parent.values.Seq()
	parentFinalErr := parent.values.Err

	out := Pipe[Out]{
		header: parent.header,
		values: seqStream[Out]{
			errFunc: parentFinalErr,
			seq:     transform(parentValues, parent.errors),
		},
	}
	return out
}

// Map returns a Pipe that applies fn to each value produced by p.
//
// Upstream transformation errors and terminal failures from the
// underlying Stream are propagated unchanged.
func Map[In, Out any](p Pipe[In], fn MapFunc[In, Out]) Pipe[Out] {
	return makeChildPipe(p, func(input iter.Seq[In], errs chan<- error) iter.Seq[Out] {
		return func(yield func(Out) bool) {
			for in := range input {
				if !yield(fn(in)) {
					return
				}
			}
		}
	})
}

// FlatMap transforms each input value using fn and returns a Pipe producing
// the flattened output values.
//
// FlatMap is equivalent to calling Flatten(Map(p, fn)).
//
// Transformation errors and terminal failures from the input pipe are propagated unchanged.
func FlatMap[In, Out any](p Pipe[In], fn MapFunc[In, []Out]) Pipe[Out] {
	return Flatten(Map(p, fn))
}

// TryMap applies fn to each value produced by p and returns a new Pipe
// emitting the successful results.
//
// If fn returns a non-nil error, the error is wrapped in a *PipeError and
// sent to the Pipe's error channel, and the corresponding value is skipped.
//
// Transformation errors produced by TryMap are added to the input pipe's
// error channel. Any terminal failure from the input pipe is propagated unchanged.
func TryMap[In, Out any](p Pipe[In], fn TryMapFunc[In, Out]) Pipe[Out] {
	return makeChildPipe(p, func(input iter.Seq[In], errs chan<- error) iter.Seq[Out] {
		return func(yield func(Out) bool) {
			for in := range input {
				result, err := fn(in)
				if err != nil {
					p.errors <- &PipeError{
						Item:   in,
						Reason: err,
					}
					continue
				}
				if !yield(result) {
					return
				}
			}
		}
	})
}

// FlatTryMap applies fn to each value produced by p and returns a new Pipe
// emitting the flattened successful results.
//
// If fn returns a non-nil error, the error is wrapped in a *PipeError and
// sent to the Pipe's error channel, and the corresponding value is skipped.
//
// Each successful result slice returned by fn is flattened, and its elements
// are emitted individually by the returned Pipe.
//
// Transformation errors produced by FlatTryMap are added to the input pipe's
// error channel. Any terminal failure from the input pipe is propagated unchanged.
//
// FlatTryMap is equivalent to calling Flatten(TryMap(p, fn)).
func FlatTryMap[In, Out any](p Pipe[In], fn TryMapFunc[In, []Out]) Pipe[Out] {
	return Flatten(TryMap(p, fn))
}

// Filter returns a Pipe that yields only the values for which predicate
// returns true.
//
// Transformation errors and terminal failures from the input pipe are propagated unchanged.
func Filter[T any](p Pipe[T], predicate Predicate[T]) Pipe[T] {
	return makeChildPipe(p, func(input iter.Seq[T], errs chan<- error) iter.Seq[T] {
		return func(yield func(T) bool) {
			for in := range input {
				if predicate(in) {
					if !yield(in) {
						return
					}
				}
			}
		}
	})
}

// Flatten converts a Pipe of slices into a Pipe of their elements,
// emitting the items of each slice in order.
//
// Transformation errors and terminal failures from the input pipe are propagated unchanged.
func Flatten[T any](p Pipe[[]T]) Pipe[T] {
	return makeChildPipe(p, func(input iter.Seq[[]T], errs chan<- error) iter.Seq[T] {
		return func(yield func(T) bool) {
			for slice := range input {
				for _, item := range slice {
					if !yield(item) {
						return
					}
				}
			}
		}
	})
}

// Chunk groups incoming values into slices of the given size and returns a Pipe producing those slices.
//
// The final chunk may be smaller than chunkSize.
//
// Chunk panics if chunkSize is not positive.
//
// Transformation errors and terminal failures from the input pipe are propagated unchanged.
func Chunk[T any](p Pipe[T], chunkSize int) Pipe[[]T] {
	if chunkSize <= 0 {
		panic("pipeline.Chunk: chunkSize must be positive")
	}

	return makeChildPipe(p, func(input iter.Seq[T], errs chan<- error) iter.Seq[[]T] {
		return func(yield func([]T) bool) {
			// NOTE: A previous version of Chunk reused the same backing slice between
			// yields. This caused aliasing issues: if a chunk was kept by the caller and
			// the buffer was reused for the next chunk, previously emitted data could
			// appear to change.
			//
			// I initially documented this and required callers to copy the slice if
			// they needed to retain it. In practice, this is not a good API:
			//   1) Callers cannot reasonably know whether retaining the slice is safe
			//      without understanding how the pipeline is built.
			//   2) Some primitives (e.g. Collect) retain values by design.
			//
			// To guarantee correct behavior in all cases, each chunk must have its own
			// backing slice.
			//
			// An alternative implementation would allocate a single backing slice
			// and yield subslices of it for each chunk.
			//
			// This approach is more cache-friendly (since the allocated memory is contiguous)
			// and reduces the number of allocations. However, it is less GC-friendly. Since
			// Go’s GC cannot reclaim parts of a backing array independently, the entire
			// buffer would remain alive as long as *any* subslice is still referenced.
			//
			// In other words, memory would only be freed once all references to all subslices
			// are gone. If some chunks are retained while others are dropped, the whole
			// backing array stays in memory.
			//
			// I have not yet decided whether this trade-off is preferable to the current
			// (simpler) implementation. In practice, it is unlikely that only some chunks
			// are retained, but if that does happen, it could lead to unexpectedly high
			// memory retention.

			accum := make([]T, 0, chunkSize)
			for i := range input {
				if len(accum) >= chunkSize {
					if !yield(accum) {
						return
					}
					accum = make([]T, 0, chunkSize)
				}

				accum = append(accum, i)
			}

			if len(accum) > 0 {
				yield(accum)
			}
		}
	})
}

// GroupBy groups consecutive input values according to a key function and
// returns a Pipe producing slices of those grouped values.
//
// GroupBy does not reorder values; it relies on the input Pipe already being
// ordered by the grouping key if consistent grouping is desired.
//
// In other words, Values are grouped only when they appear consecutively with the same key.
// When the key returned by keyFunc changes, the current group is emitted and
// a new group is started.
//
// For example, given input values:
//
//	A, A, B, B, A
//
// GroupBy will emit:
//
//	[A, A], [B, B], [A]
//
// Transformation errors and terminal failures from the input pipe are propagated unchanged.
func GroupBy[T any, K comparable](p Pipe[T], keyFunc func(T) K) Pipe[[]T] {
	return makeChildPipe(p, func(input iter.Seq[T], errs chan<- error) iter.Seq[[]T] {
		return func(yield func([]T) bool) {

			accum := make([]T, 0)
			var currentGroupKey K
			for i := range input {
				k := keyFunc(i)
				if k != currentGroupKey && len(accum) > 0 {
					if !yield(accum) {
						return
					}
					accum = make([]T, 0)
				}
				currentGroupKey = k
				accum = append(accum, i)
			}

			// yield the last group
			if len(accum) > 0 {
				yield(accum)
			}
		}
	})
}

// GroupByAggregate groups input values by key and aggregates them using user-supplied
// initialization and update callbacks, producing one aggregated output value per group.
//
// GroupByAggregate is equivalent to performing a GroupBy followed by a Map,
// but does so without allocating a slice for each group. This makes it preferred
// for pipelines where groups may be large.
//
// initFunc is called when a new group starts. It receives the first value of the
// group and should returns the initial accumulator for that group.
//
// updateFunc is called for each value in the current group, including the first one.
// It receives a pointer to the accumulator and the current input value, and should
// updates the accumulator in place.
//
// For example, to sum values in each group:
//
//	initFunc := func(v int) int {
//	    return 0 // start at 0
//	}
//
//	updateFunc := func(acc *int, v int) {
//	    *acc += v // add the value to the accumulator
//	}
//
// Like GroupBy, GroupByAggregate does not reorder input values. The input Pipe
// must already be ordered by key if consistent aggregation per key is desired.
//
// For example, with input values:
//
//	A1, A2, B1, B2, A3
//
// GroupByAggregate will emit aggregated results for:
//
//	[A1, A2], [B1, B2], [A3]
//
// Transformation errors and terminal failures from the input pipe are propagated unchanged.
func GroupByAggregate[In any, K comparable, Out any](
	p Pipe[In],
	keyFunc func(In) K,
	initFunc func(first In) Out,
	updateFunc func(acc *Out, item In)) Pipe[Out] {
	return makeChildPipe(p, func(input iter.Seq[In], errs chan<- error) iter.Seq[Out] {
		return func(yield func(Out) bool) {
			var acc *Out
			var currentGroupKey K
			for i := range input {
				k := keyFunc(i)
				if k != currentGroupKey && acc != nil {
					if !yield(*acc) {
						return
					}
					acc = nil
				}

				if acc == nil {
					// new group
					newAcc := initFunc(i)
					acc = &newAcc
				}

				currentGroupKey = k
				updateFunc(acc, i)
			}

			// yield last aggregate
			if acc != nil {
				yield(*acc)
			}

		}
	})
}

// Concat combines multiple pipes into a single Pipe that emits all values
// and pipeline errors produced by the input pipes, processing the pipes
// sequentially in the order they are provided.
//
// Values and errors produced by earlier pipes are emitted before values and
// errors from later pipes.

// Once iteration begins, all input pipes are considered used. If a pipe
// terminates early with an error, all subsequent pipes are still marked
// as used. Iterating over them will produce a Stream whose Err() returns
// ErrAlreadyConsumed.
//
// If pipes is empty, Concat returns an empty pipe.
//
// Terminal failures are reported through the concatenated value stream.
// After the stream has been fully consumed, Stream.Err() returns the first
// terminal error encountered among the input streams, if any. If a pipe
// terminates with a failure, subsequent pipes are not processed.
//
// Example:
//
//	concat := Concat(p1, p2, p3)
//
//	values, errs := concat.Results()
//
//	// drain pipeline errors
//	go func() {
//		for err := range errs {
//			log.Printf("pipeline error: %v", err)
//		}
//	}()
//
//	for v := range values.Seq() {
//		process(v)
//	}
//
//	// if non-nil, values.Err() reports the first terminal failure encountered
//	if err := values.Err(); err != nil {
//		// handle terminal failure
//	}
func Concat[T any](pipes ...Pipe[T]) Pipe[T] {

	switch len(pipes) {
	case 0:
		return Pipe[T]{}
	case 1:
		return pipes[0]
	}

	concatErrs := make(chan error)
	firstFailure := make(chan error, 1)

	onceFailure := sync.OnceValue(func() error {
		return <-firstFailure
	})

	return Pipe[T]{
		header: &header{
			errors: concatErrs,
		},
		values: seqStream[T]{
			errFunc: onceFailure,
			seq: func(yield func(T) bool) {
				// currentPipeErrDone stores the done signal of currently
				// emitting pipe
				var currentPipeErrDone chan struct{}
				defer func() {
					<-currentPipeErrDone
					close(firstFailure)
				}()

				// Consume all inputs upfront.
				//
				// From the POV of the caller, Concat should behave as a single atomic operation.
				// If the 1st stream fails, any subsequent stream should still be considered consumed.
				// NOTE: I'm not sure about that actually.
				streams := make([]Stream[T], len(pipes))
				errChans := make([]<-chan error, len(pipes))
				for i, p := range pipes {
					streams[i], errChans[i] = p.Results()
				}

				for i := range pipes {
					stream := streams[i]
					errs := errChans[i]

					currentPipeErrDone = forwardErrors(errs, concatErrs)

					for v := range stream.Seq() {
						if !yield(v) {
							return
						}
					}

					if failure := stream.Err(); failure != nil {
						firstFailure <- failure
						return
					}
				}
			},
		},
	}
}

// Merge combines multiple pipes into a single Pipe that emits all values
// and pipeline errors produced by the input pipes.
//
// Values and errors originating from different pipes may be interleaved
// and are not guaranteed to preserve the relative ordering between pipes.
//
// If pipes is empty, Merge returns an empty pipe.
//
// Terminal failures are reported through the merged value stream. After the
// stream has been fully consumed, Stream.Err() returns the first terminal
// error encountered among the merged streams, if any.
//
// Example:
//
//	merged := Merge(p1, p2, p3)
//
//	values, errs := merged.Results()
//
//	// drain pipeline errors
//	go func() {
//		for err := range errs {
//			log.Printf("pipeline error: %v", err)
//		}
//	}()
//
//	for v := range values.Seq() {
//		process(v)
//	}
//
//	// if non-nil, values.Err() reports the first terminal failure encountered
//	if err := values.Err(); err != nil {
//		// handle terminal failure
//	}
func Merge[T any](pipes ...Pipe[T]) Pipe[T] {

	switch len(pipes) {
	case 0:
		return Pipe[T]{}
	case 1:
		return pipes[0]
	}

	// NOTE: The api technically allows the caller to merge pipes of the same lineage... e.g.
	//
	//	root := somePipe()
	//	p1 := pipefn.Map(root, someMapFunc)
	//	merged := pipefn.Merge(p1, root) <== This is nonsense and will have undefined behaviour
	//
	// To catch these programmer errors, we check if any pipe shares the same header (and thus the same lineage)
	// and if so, panic.
	// FIXME: checking lineage this way is dumb and wont behave nicely with Merge since Merge
	// creates a "split" in the lineage => A merged Pipe is a new root so its *header is always a new one
	//
	//	p1 := somePipe()
	//	p2 := someOtherPipe()
	//	merged := pipefn.Merge(p1, p2)
	//	mergedBis := pipefn.Merge(merged, p1, p2) <== The suggested implementation wont catch this.

	mergedErrors := make(chan error, len(pipes))
	firstErr := make(chan error, 1)
	firstErrFunc := sync.OnceValue(func() error {
		return <-firstErr
	})

	return Pipe[T]{
		header: &header{
			errors: mergedErrors,
		},
		values: seqStream[T]{
			seq: func(yield func(T) bool) {

				defer close(firstErr)

				allStreams := make([]Stream[T], 0, len(pipes))
				allErrors := make([]<-chan error, 0, len(pipes))
				for _, p := range pipes {
					values, errs := p.Results()
					allStreams = append(allStreams, values)
					allErrors = append(allErrors, errs)
				}

				groupCtx, cancelGroup := context.WithCancel(context.Background())
				group := fanInStreams(groupCtx, allStreams...)
				go func() {
					firstErr <- group.Wait()
				}()

				errDone := fanInChans(mergedErrors, allErrors...)

				defer cancelGroup()
				for v := range group.Out() {
					if !yield(v) {
						cancelGroup()
						break
					}
				}

				// wait for the signal that mergedErrors can be closed before stopping the Seq.
				// we do it this way because the contract is:
				//
				//	all errors on pipe.header.errors *must* be sent before the Seq returns.
				//
				<-errDone
			},
			errFunc: firstErrFunc,
		},
	}
}

// forwardErrors starts a goroutine that forwards every errors from src into dst.
//
// Once src is closed, forwardErrors closes done.
func forwardErrors(src <-chan error, dst chan<- error) (done chan struct{}) {
	done = make(chan struct{})
	go func() {
		for e := range src {
			dst <- e
		}
		close(done)
	}()
	return done
}

func fanInStreams[T any](ctx context.Context, streams ...Stream[T]) *fanInGroup[T] {
	group := newFaninGroup[T](ctx)
	for _, s := range streams {
		group.Go(func(groupCtx context.Context, c chan<- T) error {
		ForLoop:
			for v := range s.Seq() {
				select {
				case <-groupCtx.Done():
					break ForLoop
				case c <- v:
				}
			}

			return s.Err()
		})
	}
	return group
}

// fanInChans combines all chans in a single dst chan.
//
// Once all inputs are closed and all values have been forwarded,
// fanInChans closes done and it is safe to close dst.
func fanInChans[T any](dst chan<- T, srcs ...<-chan T) (done chan struct{}) {
	done = make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(srcs))
	for _, ch := range srcs {
		go func() {
			defer wg.Done()
			for item := range ch {
				dst <- item
			}
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}
