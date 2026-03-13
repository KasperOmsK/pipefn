package pipefn_test

import (
	"fmt"
	"testing"

	"github.com/KasperOmsK/pipefn"

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
	pipe := pipefn.FromSeq(seqOf(1, 2, 3))

	var out []int
	for v := range pipe.Values().Seq() {
		out = append(out, v)
	}

	require.Equal(t, out, []int{1, 2, 3})
}

func TestTap(t *testing.T) {
	// Test that Tap calls tap functions in the order they are declared
	pipe := pipefn.FromSeq(seqOf(1))

	counter := 0
	actual := 0

	pipe.Tap(func(i int) {
		counter++
	})
	pipe.Tap(func(i int) {
		actual = counter
	})
	collect(pipe)
	require.NotEqual(t, actual, 0)
}

func TestTap_NoFunc(t *testing.T) {

	pipe := pipefn.FromSeq(seqOf(1))

	require.Panics(t, func() {
		pipe.Tap(nil)
	})
}

func TestYieldAfterExit(t *testing.T) {
	input := pipefn.FromSlice([]int{1})
	grouped := pipefn.GroupBy(input, func(t int) int { return t })
	mapped := pipefn.TryMap(grouped, func(in []int) (int, error) { return 0, fmt.Errorf("oops") })
	require.NotPanics(t, func() {
		collect(mapped)
	})
}
