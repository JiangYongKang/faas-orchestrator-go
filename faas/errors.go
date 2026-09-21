package faas

import (
	"errors"
	"fmt"
)

type ErrorKind string

const (
	KindUnknown              ErrorKind = "unknown"
	KindInvalidArgument      ErrorKind = "invalid_argument"
	KindNotFound             ErrorKind = "not_found"
	KindUnavailable          ErrorKind = "unavailable"
	KindConflict             ErrorKind = "conflict"
	KindTimeout              ErrorKind = "timeout"
	KindConcurrencyLimit     ErrorKind = "concurrency_limit"
	KindFunctionError        ErrorKind = "function_error"
	KindPlatformError        ErrorKind = "platform_error"
	KindRetryExhausted       ErrorKind = "retry_exhausted"
	KindUnsupportedOption    ErrorKind = "unsupported_option"
	KindProtocolIncompatible ErrorKind = "protocol_incompatible"
)

// Error is the only error type used for platform decisions. Function errors
// are wrapped with KindFunctionError so callers can distinguish them reliably.
type Error struct {
	Kind   ErrorKind
	Reason string
	Cause  error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Kind, e.Reason, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Kind, e.Reason)
}

func (e *Error) Unwrap() error { return e.Cause }

func newError(kind ErrorKind, reason string, args ...any) *Error {
	var cause error
	if len(args) > 0 {
		if err, ok := args[len(args)-1].(error); ok {
			cause = err
			args = args[:len(args)-1]
		}
	}
	return &Error{Kind: kind, Reason: fmt.Sprintf(reason, args...), Cause: cause}
}

func invalidArgument(reason string, args ...any) *Error {
	return newError(KindInvalidArgument, reason, args...)
}

func notFound(reason string, args ...any) *Error {
	return newError(KindNotFound, reason, args...)
}

func unavailable(reason string, args ...any) *Error {
	return newError(KindUnavailable, reason, args...)
}

func conflict(reason string, args ...any) *Error {
	return newError(KindConflict, reason, args...)
}

func timeoutError(reason string, args ...any) *Error {
	return newError(KindTimeout, reason, args...)
}

func concurrencyLimit(reason string, args ...any) *Error {
	return newError(KindConcurrencyLimit, reason, args...)
}

func functionError(reason string, cause error) *Error {
	return newError(KindFunctionError, reason, cause)
}

func platformError(reason string, args ...any) *Error {
	if len(args) > 0 {
		if cause, ok := args[len(args)-1].(error); ok {
			args[len(args)-1] = cause.Error()
		}
	}
	return &Error{Kind: KindPlatformError, Reason: fmt.Sprintf(reason, args...)}
}

func unsupportedOption(reason string, args ...any) *Error {
	return newError(KindUnsupportedOption, reason, args...)
}

func protocolError(reason string, args ...any) *Error {
	return newError(KindProtocolIncompatible, reason, args...)
}

// ErrorKindOf reports the platform error kind. Ordinary errors are treated as
// platform errors instead of being silently classified as function failures.
func ErrorKindOf(err error) ErrorKind {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Kind
	}
	return KindPlatformError
}

var ErrNotImplemented = errors.New("not implemented")
