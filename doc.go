/*
Package pipefn provides functional-style and composable transformations
for iter.Seq, enabling streaming pipelines without intermediate buffering.

This package is built around the concept of Pipes, a Pipe[T] represents
a lazily-evaluated stream of values of type T paired with an internal error channel.

All transformations (Map, FlatMap, Filter, and more) are provided as
package-level functions. Each transformation returns a new Pipe, allowing
pipelines to be composed through simple chaining. Values are only produced
when the resulting iter.Seq is iterated, making all pipelines demand-driven.

Errors produced by any stage flow through the pipe’s internal error channel.
All transformations inherit their input Pipe’s error channel,
so errors automatically propagate through the pipeline and can be consumed
alongside values when the pipeline is processed.

Pipes are consumed using the Results, Values, ForEach, Collect and CollectValues
methods. Consuming a Pipe is a destructive operation, once a Pipe has been consumed,
it cannot be reused or iterated again.

For more details on each type, function, and available transformation,
please refer to the corresponding package-level documentation.
*/
package pipefn

// TODO: the doc should emphasize conventions on concurrency
