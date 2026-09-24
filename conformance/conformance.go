// Package conformance holds the regression material for
// "formstream: cross-adapter streaming consistency and leak gates".
//
// The package intentionally contains no production logic. It exists so that
// `go test ./...` discovers conformance_test.go, which drives every case
// exclusively through the public entry points of formstream and its
// http/gin/echo adapters (NewParser/Register/Parse/ParseContext) and asserts
// observable semantics: parse results, causal errors, temp-file release,
// bounded buffering, and goroutine fall-back.
package conformance
