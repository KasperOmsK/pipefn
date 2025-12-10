package iterx

import (
	"iter"
)

func FromSlice[T any](in []T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, item := range in {
			if !yield(item) {
				break
			}
		}
	}
}

func FromChan[T any](in chan T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for i := range in {
			if !yield(i) {
				break
			}
		}
	}
}
