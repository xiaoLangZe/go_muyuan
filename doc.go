// Package download implements a concurrent HTTP(S) file downloader with
// resumable transfers, a globally shared worker pool, task management and a
// process-local event stream.
//
// The package is a library, not a service. Its public surface stops at Go
// primitives: tasks are managed through TaskID values and real-time
// information is delivered as a <-chan Event. It contains no HTTP handlers,
// SSE/WebSocket/gRPC types, listen addresses, authentication or serialization
// for a wire protocol. A transport layer (for example SSE) is meant to be built
// by the caller on top of Subscribe.
//
// # Concurrency model
//
// A Role owns a fixed pool of download slots ("threads") shared by every task.
// Each task has a lightweight coordinator goroutine that probes the server,
// plans chunks and pushes them into one global queue; workers consume chunks,
// so a single task can use all slots while several tasks share them fairly.
// The pool can be resized at runtime with AddWorker, RemoveWorker and
// SetThreads. A separate limit (WithMaxConcurrentTasks) bounds how many files
// run at once; further tasks wait in Pending.
//
// # Download core
//
// Capability discovery uses GET with Range: bytes=0-0. Range-capable servers
// are downloaded in parallel chunks written with WriteAt into one file (never
// O_APPEND, so concurrent writes are safe). Servers without Range support fall
// back to a single stream. Progress is tracked in a sidecar
// "<target>.dltmp.json" with a per-chunk bitmap; Resume re-probes, validates
// the sidecar and uses If-Range so a changed remote file triggers a clean
// restart instead of mixed content.
//
// # Contexts
//
// Add does not take a context. The task's context is owned by the Role and
// exposed through IDs (Pause, Cancel, Remove, Close). The context passed to
// Wait and Download only bounds the wait; canceling it does not stop the
// download.
//
// # Events
//
// Subscribe returns a channel of Events with a globally increasing Seq plus a
// cancel function. State events (added, state, removed, worker changes) are
// never silently dropped: a consumer that lets its buffer fill is disconnected
// and its channel is closed. Progress events are samples and may be coalesced.
// For reconnect support enable WithReplayBuffer and resume with WithSince.
//
// To build a consistent local view, subscribe first, then call Snapshot and
// skip live events with Seq <= snapshot.Seq.
//
// # Security
//
// By default only http and https URLs are accepted, and requests to loopback,
// private, link-local and reserved addresses are refused both before the
// request (literal hosts) and at dial time (resolved addresses, which also
// covers redirects). WithAllowPrivateHosts(true) is required for intranet
// downloads, local test servers and local proxies.
//
// Paths are resolved to absolute paths, checked for directory traversal
// (including absolute save directories when a root is configured) and for
// symbolic links before a task is registered.
package download
