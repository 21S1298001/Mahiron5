package processor

import "errors"

// ErrMirakcAribRequired is returned by collectors and scanners that depend on
// the external `mirakc-arib` CLI. Callers (e.g. the EPG gatherer) use
// `errors.Is` to decide whether the failure is a permanent configuration
// error that should not be retried.
var ErrMirakcAribRequired = errors.New("mirakc-arib is required")
