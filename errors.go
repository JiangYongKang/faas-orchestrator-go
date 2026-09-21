package faas

import "fmt"

// ErrorClass 对平台对外的错误进行归类，保证不同异常链路下
// 调用方可以依据类别做出可区分、可解释的判定。
type ErrorClass string

const (
	// ErrClassNotFound 未知函数 / 未知版本 / 未知路由。
	ErrClassNotFound ErrorClass = "not_found"
	// ErrClassInvalid 非法参数、非法版本号、非法协议选项。
	ErrClassInvalid ErrorClass = "invalid_request"
	// ErrClassConflict 路由冲突、版本状态冲突等。
	ErrClassConflict ErrorClass = "conflict"
	// ErrClassUnavailable 版本存在但不可用（Disabled / 未发布成功）。
	ErrClassUnavailable ErrorClass = "unavailable"
	// ErrClassTimeout 函数执行超过单次调用超时。
	ErrClassTimeout ErrorClass = "timeout"
	// ErrClassConcurrency 函数版本并发数已达上限。
	ErrClassConcurrency ErrorClass = "concurrency_limited"
	// ErrClassFunction 用户函数自身返回的业务错误（不可重试）。
	ErrClassFunction ErrorClass = "function_error"
	// ErrClassPlatform 平台内部错误（实例异常、协议编解码失败等，可重试）。
	ErrClassPlatform ErrorClass = "platform_error"
	// ErrClassRetryExhausted 可重试错误在策略耗尽后的最终包装。
	ErrClassRetryExhausted ErrorClass = "retry_exhausted"
)

// Error 是平台所有对外错误的统一载体。
type Error struct {
	Class  ErrorClass
	Reason string
	Cause  error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("faas: class=%s reason=%s cause=%v", e.Class, e.Reason, e.Cause)
	}
	return fmt.Sprintf("faas: class=%s reason=%s", e.Class, e.Reason)
}

func (e *Error) Unwrap() error { return e.Cause }

// newError 构造一个平台错误。
func newError(class ErrorClass, reason string) *Error {
	return &Error{Class: class, Reason: reason}
}

// wrapError 包装底层错误。
func wrapError(class ErrorClass, reason string, cause error) *Error {
	return &Error{Class: class, Reason: reason, Cause: cause}
}

// AsError 从任意 error 中提取 *Error，未命中时返回 nil。
func AsError(err error) (*Error, bool) {
	if err == nil {
		return nil, false
	}
	var pe *Error
	if e, ok := err.(*Error); ok {
		return e, true
	}
	if asF, ok := err.(interface{ Unwrap() []error }); ok {
		for _, c := range asF.Unwrap() {
			if pe, ok = AsError(c); ok {
				return pe, true
			}
		}
	}
	if uw, ok := err.(interface{ Unwrap() error }); ok {
		return AsError(uw.Unwrap())
	}
	return nil, false
}
