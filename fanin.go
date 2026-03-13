package pipefn

import (
	"context"
	"golang.org/x/sync/errgroup"
)

type fanInGroup[T any] struct {
	ctx context.Context
	g   *errgroup.Group
	out chan T
}

func newFaninGroup[T any](ctx context.Context) *fanInGroup[T] {
	g, ctx := errgroup.WithContext(ctx)

	return &fanInGroup[T]{
		ctx: ctx,
		g:   g,
		out: make(chan T),
	}
}

func (f *fanInGroup[T]) Go(fn func(context.Context, chan<- T) error) {
	f.g.Go(func() error {
		return fn(f.ctx, f.out)
	})
}

func (f *fanInGroup[T]) Out() <-chan T {
	return f.out
}

func (f *fanInGroup[T]) Wait() error {
	err := f.g.Wait()
	close(f.out)
	return err
}
