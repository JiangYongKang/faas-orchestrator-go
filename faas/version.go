package faas

import "context"

// VersionNumber is monotonic per function. Zero is never a published version.
type VersionNumber int64

const NoVersion VersionNumber = 0

type VersionState string

const (
	VersionAvailable   VersionState = "available"
	VersionUnavailable VersionState = "unavailable"
)

// HandlerFunc is the in-process function runtime. A nil handler is invalid;
// keeping it explicit prevents invocation from falling through to a default.
type HandlerFunc func(context.Context, *InvocationContext) ([]byte, error)

type InvocationContext struct {
	RequestID      string
	IdempotencyKey string
	Payload        []byte
	Attempt        int
}

type RetryPolicy struct {
	MaxAttempts int
	Retryable   map[FailureClass]bool
}

// FunctionSpec is copied into an immutable published version.
type FunctionSpec struct {
	Name           string
	Handler        HandlerFunc
	Command        *LocalCommand
	TimeoutMillis  int
	MaxConcurrency int
	Retries        RetryPolicy
}

type PublishedVersion struct {
	Function string
	Number   VersionNumber
	State    VersionState
	Spec     FunctionSpec
}

type FunctionRecord struct {
	Name     string
	Versions []PublishedVersion
	Current  VersionNumber
	Next     VersionNumber
}

func (v PublishedVersion) CanServe() bool {
	return v.Number != NoVersion && v.State == VersionAvailable && (v.Spec.Handler != nil || v.Spec.Command != nil)
}

func (v PublishedVersion) clone() PublishedVersion {
	spec := v.Spec
	if v.Spec.Retries.Retryable != nil {
		retryable := make(map[FailureClass]bool, len(v.Spec.Retries.Retryable))
		for class, allowed := range v.Spec.Retries.Retryable {
			retryable[class] = allowed
		}
		spec.Retries.Retryable = retryable
	}
	v.Spec = spec
	return v
}
