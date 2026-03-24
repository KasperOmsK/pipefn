package pipefn_test

import (
	"fmt"
	"iter"
	"strings"
	"sync"

	"github.com/KasperOmsK/pipefn"
)

type Item struct {
	ID string
}

type Event struct {
	Type  string
	Items []Item
}

// Example demonstrates a relatively complex pipeline that parses items from multiple sources
func Example() {

	var (
		// stream is a fake stream that will yield some data but then report
		// a terminal failure.
		stream pipefn.Stream[string] = newStubStream([]string{
			"purchase:9999",
		}, fmt.Errorf("fake error"))
	)

	// Create a pipe from a slice
	inputsFromSlice := pipefn.FromSlice([]string{
		"purchase:1001,1002,1003",
		"refund:2001",
		"purchase:1004,1005",
		"purchase:", // invalid item (empty ID)
	})

	// ... or from an iter.Seq
	inputsFromSeq := pipefn.FromSeq(func(yield func(string) bool) {
		yield("refund:420")
	})

	// ... or from a channel
	ch := make(chan string)
	go func() {
		ch <- ":1000" // invalid item (no event type)
		ch <- "refund:1,2,3"
		close(ch)
	}()
	inputsFromChan, _ := pipefn.FromChan(ch)

	// ... or from the Stream interface
	inputsFromStream := pipefn.From(stream)

	// Merge multiple pipes of the same type.
	//
	// if any input pipe fails, the merged pipe will also fail with the first encountered failure.
	allInputs := pipefn.Merge(inputsFromSeq, inputsFromSlice, inputsFromStream, inputsFromChan)

	// Map applies deterministic transformations that cannot fail.
	// Transformations are simple functions, making it easy to reuse existing code.
	trimmed := pipefn.Map(allInputs, strings.TrimSpace)

	// TryMap applies transformations that may fail.
	// Errors are forwarded to the Pipe's error channel.
	events := pipefn.TryMap(trimmed, func(line string) (Event, error) {
		return ParseEvent(line)
	})

	// All common FP-style transformation are available.

	purchases := pipefn.Filter(events, func(e Event) bool {
		return e.Type == "purchase"
	})

	items := pipefn.FlatMap(purchases, func(e Event) []Item {
		return e.Items
	})

	// TryMap can act as a validation filter with error reporting
	validItems := pipefn.TryMap(items, func(it Item) (Item, error) {
		if it.ID == "" {
			return Item{}, fmt.Errorf("missing item ID")
		}
		return it, nil
	})

	batches := pipefn.Chunk(validItems, 2)

	// retreive the value stream and the pipeline errors channel
	values, errs := batches.Results()

	// Pipeline errors *must* be consumed concurrently to avoid blocking the pipeline.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errs {
			// errors emitted errs will always be of type *PipeError
			perr := err.(*pipefn.PipeError)
			fmt.Printf("pipeline error: item: %v, reason: %s\n", perr.Item, perr.Reason)
		}
	}()

	// Or alternatively, if you're only interested in successful values of the pipeline...
	//	values := batches.Values()
	// ... will only return the value stream and silently discard pipeline errors.

	batchCount := 0
	for batch := range values.Seq() {
		fmt.Println("batch:", batchCount)
		err := ProcessBatch(batch)
		if err != nil {
			break // Stopping consumption halts the pipeline
		}
		batchCount++
	}
	wg.Wait()

	// values.Err() returns any terminal failure of the pipe.
	//
	// A non-nil error here indicates that at least one input stream failed.
	//
	// In our example, let's say that such an error means the work done by ProcessBatch
	// should be discarded,
	if err := values.Err(); err != nil {
		Rollback()
	} else {
		Commit()
	}
}

func Rollback() {
	fmt.Println("work rollbacked :()")
}

func Commit() {
	fmt.Println("work commited !")
}

func ParseEvent(line string) (Event, error) {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return Event{}, fmt.Errorf("invalid event format: %q", line)
	}

	eventType := strings.TrimSpace(parts[0])
	rawItems := strings.TrimSpace(parts[1])

	var items []Item
	if rawItems == "" {
		return Event{}, fmt.Errorf("invalid event format: no items")
	}

	for _, id := range strings.Split(rawItems, ",") {
		items = append(items, Item{
			ID: strings.TrimSpace(id),
		})
	}

	return Event{
		Type:  eventType,
		Items: items,
	}, nil
}

func ProcessBatch(items []Item) error {
	for _, it := range items {
		fmt.Println(it)
	}
	return nil
}

// newStubStream returns a pipefn.Stream that simulates reading from a file with a terminal failure
func newStubStream(data []string, err error) pipefn.Stream[string] {
	return &stubStream{
		stubData: func(yield func(string) bool) {
			for _, e := range data {
				if !yield(e) {
					return
				}
			}
		},
		failure: err,
	}
}

type stubStream struct {
	stubData iter.Seq[string]
	failure  error
}

func (fs *stubStream) Seq() iter.Seq[string] {
	return fs.stubData
}

func (fs *stubStream) Err() error {
	return fs.failure
}
