package pipefn_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/KasperOmsK/pipefn"

	"github.com/stretchr/testify/require"
)

func TestFrom(t *testing.T) {
	// TODO: test that all FromXXX behave identically
	pipe := pipefn.FromSeq(seqOf(1, 2, 3))

	var out []int
	for v := range pipe.Values().Seq() {
		out = append(out, v)
	}

	require.Equal(t, out, []int{1, 2, 3})
}

func TestFrom_PanicsIfNilStream(t *testing.T) {
	require.Panics(t, func() {
		pipefn.From[int](nil)
	})
}

func TestFromSeq_PanicsIfNilSeq(t *testing.T) {
	require.Panics(t, func() {
		pipefn.FromSeq[int](nil)
	})
}

func TestPipe_ZeroValuePipeUsable(t *testing.T) {
	pipe := pipefn.Pipe[int]{}

	var (
		values []int
		errs   []error
		err    error
	)
	require.NotPanics(t, func() {
		values, errs, err = collect(pipe)
	})

	require.Empty(t, values)
	require.Empty(t, errs)
	require.NoError(t, err)
}

func TestPipe_MultipleResultsReturnsErrorStream(t *testing.T) {
	pipe := pipefn.FromSlice([]int{1, 2, 3})

	type results struct {
		values []int
		errors []error
		err    error
	}

	var resultsMu sync.RWMutex
	var allResults []results

	var wg sync.WaitGroup
	const numTests = 100
	wg.Add(numTests)
	for range numTests {
		go func() {
			defer wg.Done()

			resultsMu.Lock()
			values, errs, err := collect(pipe)
			allResults = append(allResults, results{
				values: values,
				errors: errs,
				err:    err,
			})
			resultsMu.Unlock()
		}()
	}
	wg.Wait()

	var (
		okStreams, errorsCount, failedStreams int
	)
	for _, r := range allResults {
		if len(r.values) != 0 {
			okStreams++
		}
		if len(r.errors) != 0 {
			errorsCount++
		}
		if r.err != nil {
			failedStreams++
		}
	}

	require.Equal(t, 1, okStreams, "wrong number of ok stream")                  // expect one OK stream
	require.Zero(t, 0, errorsCount, "unexpected errors")                         // no error
	require.Equal(t, numTests-1, failedStreams, "wrong number of failed stream") // all other stream should have terminal failures

	//t.Errorf("TODO: implement")
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
