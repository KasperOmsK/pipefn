package pipefn_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/KasperOmsK/pipefn"

	"github.com/stretchr/testify/require"
)

func TestFrom(t *testing.T) {
	pipe := pipefn.From(&seqStream[int]{
		seq: func(yield func(int) bool) {
			for i := range 4 {
				if !yield(i) {
					return
				}
			}
		},
	})

	values, errs, err := collect(pipe)
	require.Equal(t, []int{0, 1, 2, 3}, values)
	require.Empty(t, errs)
	require.NoError(t, err)
}

func TestFrom_PanicsIfNilStream(t *testing.T) {
	require.Panics(t, func() {
		pipefn.From[int](nil)
	})
}

func TestFromSeq(t *testing.T) {
	pipe := pipefn.FromSeq(seqOf(0, 1, 2, 3))

	values, errs, err := collect(pipe)

	require.Equal(t, []int{0, 1, 2, 3}, values)
	require.Empty(t, errs)
	require.NoError(t, err)
}

func TestFromSeq_PanicsIfNilSeq(t *testing.T) {
	require.Panics(t, func() {
		pipefn.FromSeq[int](nil)
	})
}

func TestFromSlice(t *testing.T) {
	data := []int{0, 1, 2, 3}
	pipe := pipefn.FromSlice(data)

	values, errs, err := collect(pipe)
	require.Equal(t, data, values)
	require.Empty(t, errs)
	require.NoError(t, err)
}

func TestFromSlice_Empty(t *testing.T) {
	pipe := pipefn.FromSlice[int](nil)
	requireEmpty(t, pipe)
}

func TestFromChan(t *testing.T) {

	t.Run("drains channel", func(t *testing.T) {
		ch := make(chan int)
		go func() {
			for i := range 4 {
				ch <- i
			}
			close(ch)
		}()
		pipe, done := pipefn.FromChan(ch)

		values, errs, err := collect(pipe)
		require.Equal(t, []int{0, 1, 2, 3}, values)
		require.Empty(t, errs)
		require.NoError(t, err)
		requireCompletesIn(t, func() {
			// FromChan should have closed done
			<-done
		}, time.Second)
	})

	t.Run("closes done early on break", func(t *testing.T) {
		ch := make(chan int)
		pipe, done := pipefn.FromChan(ch)
		go func() {
			defer close(ch)
			for i := range 4 {
				select {
				case ch <- i:
				case <-done:
					return
				}
			}
		}()
		values := pipe.Values()

		// break early
		for range values.Seq() {
			break
		}

		require.NoError(t, values.Err())
		requireCompletesIn(t, func() {
			// FromChan should have closed done after our break
			<-done
		}, time.Second)
	})

}

func TestPipe_ZeroValuePipeUsable(t *testing.T) {
	zeroPipe := pipefn.Pipe[int]{}

	var (
		values []int
		errs   []error
		err    error
	)
	require.NotPanics(t, func() {
		values, errs, err = collect(zeroPipe)
	})

	require.Empty(t, values)
	require.Empty(t, errs)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		pipefn.Map(zeroPipe, func(in int) int { return in })
	})
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
	grouped := pipefn.GroupByKey(input, func(t int) int { return t })
	mapped := pipefn.TryMap(grouped, func(in []int) (int, error) { return 0, fmt.Errorf("oops") })
	require.NotPanics(t, func() {
		collect(mapped)
	})
}
