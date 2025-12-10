/*
Package pipefn provides functional-style and composable transformations
for iter.Seq, enabling streaming pipelines without intermediate buffering.

This package is built around the concept of Pipes, a Pipe[T] represents
a lazily-evaluated stream of values of type T.

All transformations (Map, FlatMap, Filter, and more) are provided as
package-level functions. Each transformation returns a new Pipe, allowing
pipelines to be composed through simple chaining. Values are only produced
when the resulting iter.Seq is iterated, making all pipelines demand-driven.

Errors produced by any stage flow through the pipe’s internal error channel.
All transformations inherit their input Pipe’s error channel,
so errors automatically propagate through the pipeline and can be consumed
alongside values when the pipeline is processed.

Example of a simple pipeline:

	// Wrap iter.Seqs into Pipes
	input1 := pipfn.From(IterateFile("events-2023.log"))
	input2 := pipfn.From(IterateFile("events-2024.log"))

	// Merge multiple pipes of the same type.
	p := pipefn.Merge(input1, input2)

	// Map applies deterministic transformations that cannot fail.
	// Transformations are simple functions, making it easy to reuse existing code.
	trimmed := pipefn.Map(p, strconv.TrimSpace)

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

	batches := pipefn.Chunk(validItems, 20)

	// Convert Pipe back to iter.Seqs for consumption
	vals, errs := pipefn.Results(batches)

	// Errors *must* be consumed concurrently to avoid blocking the pipeline.
	go func() {
	    for err := range errs {
		log.Println("pipeline error:", err)
	    }
	}()

	for batch := range vals {
		err := ProcessBatch(batch)
		if err != nil{
			break // Stopping consumption halts the pipeline
		}
	}

For more details on each type, function, and available transformation,
please refer to the corresponding package-level documentation.
Each function is documented with usage examples and notes on error propagation.
*/
package pipefn

// TODO: the doc should emphasize conventions on concurrency
