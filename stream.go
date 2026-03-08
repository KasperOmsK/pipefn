package pipefn

import "iter"

// Stream wraps an iter.Seq whose iteration may terminate with an error.
//
// The sequence of values is provided by the Seq field. After the
// sequence has been fully consumed, Err reports any terminal error that
// occurred during iteration. If iteration completed successfully, Err
// returns nil.
//
// Err should be checked after the sequence has been exhausted.
type Stream[T any] struct {
	iter.Seq[T]
	errFunc func() error
}

// Err returns any failure encountered during iteration.
//
// If iteration completed without error, Err() returns nil.
func (s Stream[T]) Err() error {
	if s.errFunc != nil {
		return s.errFunc()
	}
	return nil
}
