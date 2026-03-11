package pipefn_test

import (
	"fmt"
	"testing"

	"github.com/KasperOmsK/pipefn"
	"github.com/KasperOmsK/pipefn/internal/iterx"

	"github.com/stretchr/testify/require"
)

type stubCursor struct {
	head     int
	elems    []int
	finalErr error
}

func (c *stubCursor) Next() bool {
	return c.head < len(c.elems)
}

func (c *stubCursor) Value() int {
	head := c.head
	c.head++
	return c.elems[head]
}

func (c *stubCursor) Err() error {
	return c.finalErr
}

func TestFrom(t *testing.T) {
	pipe := pipefn.From(seqOf(1, 2, 3))

	var out []int
	for v := range pipe.Values().Seq {
		out = append(out, v)
	}

	require.Equal(t, out, []int{1, 2, 3})
}

func TestFromCursor(t *testing.T) {
	cursor := &stubCursor{
		elems: []int{1, 2, 3},
	}
	pipe := pipefn.FromCursor(cursor)
	values, errs, err := collect(pipe)
	require.ElementsMatch(t, values, cursor.elems)
	require.Empty(t, errs)
	require.NoError(t, err)
}

func TestFromCursor_ForwardsError(t *testing.T) {
	cursor := &stubCursor{
		elems:    []int{1, 2, 3},
		finalErr: fmt.Errorf("cursor error"),
	}
	pipe := pipefn.FromCursor(cursor)
	values, errs, err := collect(pipe)
	require.ElementsMatch(t, values, cursor.elems)
	require.Empty(t, errs)
	require.ErrorIs(t, err, cursor.finalErr)
}

func TestTap(t *testing.T) {
	// Test that Tap calls tap functions in the order they are declared
	pipe := pipefn.From(seqOf(1))

	counter := 0

	pipe.Tap(func(i int) {
		counter++
	})
	pipe.Tap(func(i int) {
		require.NotEqual(t, counter, 0)
	})

	collect(pipe)
}

func TestTap_NoFunc(t *testing.T) {

	pipe := pipefn.From(seqOf(1))

	require.Panics(t, func() {
		pipe.Tap(nil)
	})
}

func TestYieldAfterExit(t *testing.T) {
	input := pipefn.From(iterx.FromSlice([]int{1}))
	grouped := pipefn.GroupBy(input, func(t int) int { return t })
	mapped := pipefn.TryMap(grouped, func(in []int) (int, error) { return 0, fmt.Errorf("oops") })
	require.NotPanics(t, func() {
		collect(mapped)
	})
}
