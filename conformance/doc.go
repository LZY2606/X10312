// Package conformance anchors the cross-adapter streaming conformance
// harness for formstream.
//
// The harness itself lives in this package's tests: shared fixtures
// (empty part, oversized headers, unicode filenames, repeated file
// fields, slow consumers, failing readers, prefix-only consumers) are
// driven through every public adapter (core formstream.Parser, net/http,
// gin, echo) and checked for identical observable semantics, causal
// error preservation, bounded buffering, and consistent resource
// release. No production code is duplicated here; every case enters
// through the public entry points.
package conformance
