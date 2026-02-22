package pipefn_test

import (
	"github.com/KasperOmsK/pipefn"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFrom(t *testing.T) {
	pipe := pipefn.From(seqOf(1, 2, 3))

	var out []int
	for v := range pipe.Values() {
		out = append(out, v)
	}

	require.Equal(t, out, []int{1, 2, 3})
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
