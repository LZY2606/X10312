// Package conformance holds the regression material for
// "formstream: cross-adapter streaming consistency and leak gates".
//
// Every case enters through the public entry points only
// (formstream.NewParser/Register/Parse and the http/echo/gin adapters)
// and asserts observable semantics plus internal invariants:
// bounded in-flight buffering, read/close accounting, goroutine
// settlement, temporary-file cleanup and causal error preservation.
package conformance
