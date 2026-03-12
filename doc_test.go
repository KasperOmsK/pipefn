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

// Example demonstrates a relatively complex pipeline that parses items from (fake) event files
func Example() {
	// Wrap iter.Seqs into Pipes
	input1 := pipefn.FromSeq(IterateFile("events-2023.log"))
	input2 := pipefn.FromSeq(IterateFile("events-2024.log"))

	// Merge multiple pipes of the same type.
	p := pipefn.Merge(input1, input2)

	// Map applies deterministic transformations that cannot fail.
	// Transformations are simple functions, making it easy to reuse existing code.
	trimmed := pipefn.Map(p, strings.TrimSpace)

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
	for batch := range values.Seq {
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
	// A non-nil error here indicates that the pipeline as a whole failed,
	// regardless of any items that were successfully processed.
	//
	// In this example, such an error means the work done by ProcessBatch
	// should be discarded, so we call Rollback(). Otherwise, if no terminal
	// failure occurred, we can safely commit the processed batches.
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

func IterateFile(path string) iter.Seq[string] {
	return func(yield func(string) bool) {
		var lines []string

		switch path {
		case "events-2023.log":
			lines = []string{
				"purchase:1001,1002,1003",
				"refund:2001",
				"purchase:1004,1005",
				"purchase:", // invalid item (empty ID)
			}
		case "events-2024.log":
			lines = []string{
				"purchase:3001,3002",
				"invalid-line-without-colon",
				"purchase:3003",
			}
		default:
			lines = []string{}
		}

		for _, line := range lines {
			if !yield(line) {
				return
			}
		}
	}
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
