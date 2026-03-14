package pipefn

import (
	"iter"
)

// Stream represents a generic data stream.
//
// A Stream produces a sequence of values of type T and may terminate with an
// error. Consumers iterate over the values using the Seq method and must check
// Err after iteration completes to determine whether the stream ended normally
// or due to an error.
//
// Typical usage:
//
//	s := getStream()
//	for v := range s.Seq() {
//	    // process v
//	}
//	if err := s.Err(); err != nil {
//	    // handle stream error
//	}
//
// The sequence returned by Seq should be consumed before calling Err to ensure
// that the stream has fully completed.
type Stream[T any] interface {
	// Seq returns the iterator that yields elements of the stream.
	Seq() iter.Seq[T]

	// Err returns the terminal error encountered while producing the stream.
	// It returns nil if the stream completed successfully. Err should be
	// checked only after the sequence returned by Seq has been fully consumed.
	Err() error
}

type seqStream[T any] struct {
	seq     iter.Seq[T]
	errFunc func() error
}

func (ss seqStream[T]) Seq() iter.Seq[T] {
	return ss.seq
}

func (ss seqStream[T]) Err() error {
	if ss.errFunc != nil {
		return ss.errFunc()
	}
	return nil
}

func errorStream[T any](eofErr error) Stream[T] {
	return seqStream[T]{
		seq: func(yield func(T) bool) {},
		errFunc: func() error {
			return eofErr
		},
	}
}

func emptyStream[T any]() Stream[T] {
	return seqStream[T]{
		seq: func(yield func(T) bool) {},
	}
}
