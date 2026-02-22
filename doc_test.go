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
	input1 := pipefn.From(IterateFile("events-2023.log"))
	input2 := pipefn.From(IterateFile("events-2024.log"))

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

	// Convert Pipe back to iter.Seq for consumption
	vals, errs := batches.Results()

	// Errors *must* be consumed concurrently to avoid blocking the pipeline.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errs {
			fmt.Println("pipeline error:", err)
		}
	}()

	batchCount := 0
	for batch := range vals {
		fmt.Println("batch:", batchCount)
		err := ProcessBatch(batch)
		if err != nil {
			break // Stopping consumption halts the pipeline
		}
		batchCount++
	}
	wg.Wait()
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
