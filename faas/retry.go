package faas

import "strconv"

type FailureClass string

const (
	FailureNone      FailureClass = ""
	FailureFunction  FailureClass = "function"
	FailureTimeout   FailureClass = "timeout"
	FailurePlatform  FailureClass = "platform"
	FailureThrottled FailureClass = "throttled"
)

type RetryDecision struct {
	Attempt int
	Class   FailureClass
	Retry   bool
	Reason  string
}

func ClassifyError(err error) FailureClass {
	if err == nil {
		return FailureNone
	}
	switch ErrorKindOf(err) {
	case KindFunctionError:
		return FailureFunction
	case KindTimeout:
		return FailureTimeout
	case KindConcurrencyLimit:
		return FailureThrottled
	case KindPlatformError:
		return FailurePlatform
	default:
		// Validation, routing and version failures are deterministic and must
		// not be retried by the execution policy.
		return FailurePlatform
	}
}

func normalizeRetryPolicy(policy RetryPolicy) RetryPolicy {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	if policy.Retryable == nil {
		policy.Retryable = map[FailureClass]bool{}
	}
	return policy
}

// ShouldRetry evaluates an already-executed attempt. attempt is one-based. The
// explicit policy map is authoritative: absent classes are not retried.
func ShouldRetry(policy RetryPolicy, class FailureClass, attempt int) (bool, string) {
	policy = normalizeRetryPolicy(policy)
	if class == FailureNone {
		return false, "no failure"
	}
	if attempt < 1 {
		return false, "invalid attempt number"
	}
	if attempt >= policy.MaxAttempts {
		return false, "attempt " + strconv.Itoa(attempt) + " reached max attempts " + strconv.Itoa(policy.MaxAttempts)
	}
	if !policy.Retryable[class] {
		return false, "failure class " + string(class) + " is not retryable"
	}
	return true, "retryable failure class " + string(class) + "; attempts remaining"
}
