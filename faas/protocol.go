package faas

type ProtocolVersion string

const (
	ProtocolV1 ProtocolVersion = "faas.protocol/v1"
	ProtocolV2 ProtocolVersion = "faas.protocol/v2"
)

// InvokeRequest is the stable local kernel protocol. A zero Protocol is
// interpreted as V1 only after explicit validation, rather than being ignored.
type InvokeRequest struct {
	Protocol       ProtocolVersion
	Route          string
	Function       string
	Version        VersionNumber
	Payload        []byte
	IdempotencyKey string
	Options        CallOptions
}

// CallOptions contains only options understood by this kernel. Any unknown
// option passed by a decoded caller must be copied into UnsupportedFields so
// validation rejects it.
type CallOptions struct {
	TimeoutMillis     int
	MaxConcurrency    int
	Retryable         *bool
	MaxAttempts       int
	UnsupportedFields map[string]any
}

type InvokeResponse struct {
	Function string
	Version  VersionNumber
	Attempt  int
	Payload  []byte
}

type DecisionLog struct {
	RequestID string
	Input     []byte
	Decision  string
	Reason    string
}

type Logger interface {
	Log(DecisionLog)
}

type noopLogger struct{}

func (noopLogger) Log(DecisionLog) {}

// ValidateRequest enforces protocol compatibility before resolving a route or
// touching the instance pool.
func ValidateRequest(req InvokeRequest) error {
	switch req.Protocol {
	case "":
		// V1 omitted the protocol field.
	case ProtocolV1, ProtocolV2:
	default:
		return protocolError("unsupported protocol %q", string(req.Protocol))
	}
	for name := range req.Options.UnsupportedFields {
		return unsupportedOption("protocol option %q is not supported", name)
	}
	if req.Route == "" && req.Function == "" {
		return invalidArgument("invocation must specify route or function")
	}
	if req.Route != "" && req.Function != "" {
		return invalidArgument("invocation cannot specify both route and function")
	}
	if req.Function != "" && req.Version < NoVersion {
		return invalidArgument("function version cannot be negative")
	}
	if req.Options.TimeoutMillis < 0 {
		return invalidArgument("call timeout cannot be negative")
	}
	if req.Options.MaxConcurrency < 0 {
		return invalidArgument("call concurrency override cannot be negative")
	}
	if req.Options.MaxAttempts < 0 {
		return invalidArgument("call max attempts override cannot be negative")
	}
	if req.Protocol == ProtocolV1 {
		// V1 had no per-call retry override.
		if req.Options.Retryable != nil || req.Options.MaxAttempts != 0 {
			return unsupportedOption("protocol v1 does not support per-call retry overrides")
		}
	}
	return nil
}
