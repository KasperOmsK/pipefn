package pipefn

import (
	"iter"
)

type Stream[T any] interface {
	Seq() iter.Seq[T]
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
