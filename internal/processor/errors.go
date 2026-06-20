package processor

import "errors"

// ErrMirakcAribRequired is returned by processor adapters that still depend on
// the external `mirakc-arib` CLI.
var ErrMirakcAribRequired = errors.New("mirakc-arib is required")
