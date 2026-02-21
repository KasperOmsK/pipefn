package pipefn

import (
	"iter"
	"pipefn/internal/iterx"
	"sync"
)

type (

	// MapFunc is a pure mapping function used by Map that transforms a value
	// of type In into a value of type Out.
	MapFunc[In, Out any] func(in In) Out

	// TryMapFunc is a mapping function that may return an error.
	//
	// Errors are forwarded to the Pipe's error channel while
	// successful values continue through the pipeline.
	TryMapFunc[In, Out any] func(in In) (Out, error)

	// Predicate represents a filtering function that returns true when the
	// provided value should be included in the output stream.
	Predicate[T any] func(item T) bool
)

// Map transforms each input value using fn and returns a new Pipe producing
// the mapped values.
//
// Errors from the input Pipe are preserved.
func Map[In, Out any](p Pipe[In], fn MapFunc[In, Out]) Pipe[Out] {
	return Pipe[Out]{
		errors: p.errors,
		seq: func(yield func(Out) bool) {
			for in := range p.seq {
				if !yield(fn(in)) {
					return
				}
			}
		},
	}
}

// FlatMap transforms each input value using fn and returns a Pipe producing
// the flattened output values.
//
// FlatMap is equivalent to calling Flatten(Map(p, fn)).
//
// Errors from the input Pipe are preserved.
func FlatMap[In, Out any](p Pipe[In], fn MapFunc[In, []Out]) Pipe[Out] {
	return Flatten(Map(p, fn))
}

// TryMap transforms each input value using fn, forwarding any non-nil
// errors onto the Pipe's error channel and yielding only successful results.
//
// Errors from the input Pipe are preserved.
func TryMap[In, Out any](p Pipe[In], fn TryMapFunc[In, Out]) Pipe[Out] {
	return Pipe[Out]{
		errors: p.errors,
		seq: func(yield func(Out) bool) {
			for in := range p.seq {
				result, err := fn(in)
				if err != nil {
					p.errors <- PipelineError{
						Item:   in,
						Reason: err,
					}
					continue
				}
				if !yield(result) {
					return
				}
			}
		},
	}
}

// FlatTryMap transforms each input value using fn and returns a Pipe producing
// the flattened output values.
//
// Any non-nil error returned by fn is forwarded to the Pipe’s error channel.
//
// FlatTryMap is equivalent to calling Flatten(TryMap(p, fn)).
//
// Errors from the input Pipe are preserved.
func FlatTryMap[In, Out any](p Pipe[In], fn TryMapFunc[In, []Out]) Pipe[Out] {
	return Flatten(TryMap(p, fn))
}

// Filter returns a Pipe that yields only the values for which predicate
// returns true.
//
// Errors from the input Pipe are preserved.
func Filter[T any](p Pipe[T], predicate Predicate[T]) Pipe[T] {
	return Pipe[T]{
		errors: p.errors,
		seq: func(yield func(T) bool) {
			for in := range p.seq {
				if predicate(in) {
					if !yield(in) {
						return
					}
				}
			}
		},
	}
}

// Flatten converts a Pipe of slices into a Pipe of their elements,
// emitting the items of each slice in order.
//
// Errors from the input Pipe are preserved.
func Flatten[T any](p Pipe[[]T]) Pipe[T] {
	out := Pipe[T]{
		errors: p.errors,
		seq: func(yield func(T) bool) {
			for slice := range p.seq {
				for _, item := range slice {
					if !yield(item) {
						return
					}
				}
			}
		},
	}
	return out
}

// Chunk groups incoming values into slices of the given size and returns a Pipe producing those slices.
//
// The final chunk may be smaller than chunkSize.
//
// Chunk panics if chunkSize is not positive.
//
// Errors from the input Pipe are preserved.
func Chunk[T any](p Pipe[T], chunkSize int) Pipe[[]T] {
	if chunkSize <= 0 {
		panic("pipeline.Chunk: chunkSize must be positive")
	}

	return Pipe[[]T]{
		errors: p.errors,
		seq: func(yield func([]T) bool) {

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
			for i := range p.seq {
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
		},
	}
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
// Errors from the input Pipe are preserved.
func GroupBy[T any, K comparable](p Pipe[T], keyFunc func(T) K) Pipe[[]T] {
	return Pipe[[]T]{
		errors: p.errors,
		seq: func(yield func([]T) bool) {
			accum := make([]T, 0)
			var currentGroupKey K
			for i := range p.seq {
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
		},
	}
}

// TODO: GroupByAggregate is the equivalent of GroupBy + FoldLeft, not GroupBy + map !!
// FoldLeft e.g. => val sum = ints.foldLeft(10) { (accumalor, currentElement) => accumulator + currentElement}

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
// updateFunc is called for each value in the current group. It receives a pointer
// to the accumulator and the current input value, and should updates the accumulator in place.
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
// Errors from the input Pipe are preserved.
func GroupByAggregate[In any, K comparable, Out any](
	p Pipe[In],
	keyFunc func(In) K,
	initFunc func(first In) Out,
	updateFunc func(acc *Out, item In)) Pipe[Out] {

	return Pipe[Out]{
		errors: p.errors,
		seq: func(yield func(Out) bool) {
			var acc *Out
			var currentGroupKey K
			for i := range p.seq {
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

		},
	}
}

// Merge combines multiple pipes into a single pipe that yields all values
// produced by the input pipes.
//
// Values from different pipes may appear in any order. Errors from all input
// pipes are merged into the returned pipe's error channel.
func Merge[T any](pipes ...Pipe[T]) Pipe[T] {
	if len(pipes) == 0 {
		return From(iterx.FromSlice(make([]T, 0)))
	}

	if len(pipes) == 1 {
		return pipes[0]
	}

	allErrors := make([]chan PipelineError, 0, len(pipes))
	allValues := make([]iter.Seq[T], 0, len(pipes))

	for _, p := range pipes {
		allValues = append(allValues, p.seq)
		allErrors = append(allErrors, p.errors)
	}

	return Pipe[T]{
		seq:    mergeIterators(allValues...),
		errors: mergeChans(allErrors...),
	}
}

func mergeIterators[T any](ins ...iter.Seq[T]) iter.Seq[T] {

	return func(yield func(T) bool) {
		mergedItemsChan := make(chan T)
		done := make(chan struct{})
		var wg sync.WaitGroup

		wg.Add(len(ins))

		go func() {
			wg.Wait()
			close(mergedItemsChan)
		}()

		for _, in := range ins {
			go func(wg *sync.WaitGroup, it iter.Seq[T]) {
				defer wg.Done()
				for item := range it {
					select {
					case mergedItemsChan <- item:
					case <-done:
						return
					}
				}
			}(&wg, in)
		}
		for i := range mergedItemsChan {
			if !yield(i) {
				// tell input iterators to stop producing
				close(done)
				go func() {
					// drain remaining items that were already sent
					for range mergedItemsChan {
					}
				}()

				return
			}
		}
	}
}

func mergeChans[T any](chans ...chan T) chan T {
	out := make(chan T)

	var mergeWg sync.WaitGroup
	mergeWg.Add(len(chans))
	for _, ch := range chans {
		go func() {
			defer mergeWg.Done()
			for e := range ch {
				out <- e
			}
		}()
	}

	go func() {
		mergeWg.Wait()
		close(out)
	}()
	return out
}
