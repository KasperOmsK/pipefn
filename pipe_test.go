package pipefn_test

import (
	"fmt"
	"testing"

	"github.com/KasperOmsK/pipefn"
	"github.com/KasperOmsK/pipefn/internal/iterx"

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

func TestTap_NoFunc(t *testing.T) {

	pipe := pipefn.From(seqOf(1))

	require.Panics(t, func() {
		pipe.Tap(nil)
	})
}

func TestTryMapErrorChannelRace(t *testing.T) {
	const iterations = 10000

	for range iterations {

		// Large input to increase scheduling interleavings
		inputData := make([]int, 1000)
		for i := range 999 {
			inputData[i] = i
		}
		inputData[999] = 42 // sentinel for failure

		input := pipefn.From(iterx.FromSlice(inputData))

		// Force eager draining
		grouped := pipefn.GroupBy(input, func(i int) int {
			return i / 10
		})

		// Fail only on the LAST group
		mapped := pipefn.TryMap(grouped, func(in []int) (int, error) {
			sum := 0
			for _, v := range in {
				sum += v
			}
			if in[len(in)-1] == 42 {
				return 0, fmt.Errorf("late failure")
			}
			return sum, nil
		})

		it, errs := mapped.Results()

		go func() {
			for range it {
				break
			}
		}()

		for range errs {
		}
	}
}
