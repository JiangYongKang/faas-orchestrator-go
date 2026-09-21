package faas

import (
	"encoding/base64"
	"testing"
	"time"
)

// base64Enc 对字符串做标准 base64 编码，用于构造报文 payload。
func base64Enc(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

// testCfg 生成一份基础可用的版本配置，便于各用例按需覆盖字段。
func testCfg(handler string) VersionConfig {
	return VersionConfig{
		HandlerName:    handler,
		Timeout:        500 * time.Millisecond,
		MaxConcurrency: 4,
		Retry:          RetryPolicy{MaxAttempts: 1, Backoff: time.Millisecond},
	}
}

// logCase 打印测试输入与判定依据，满足"单测日志中打印调用输入与判定依据"。
func logCase(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Logf("[判定依据] "+format, args...)
}

// asErr 丢弃成功返回值，只取 error，便于做纯错误断言。
func asErr(_ any, err error) error { return err }

// assertClass 断言错误非空且属于预期错误类别，并打印判定依据。
func assertClass(t *testing.T, err error, want ErrorClass) {
	t.Helper()
	if err == nil {
		t.Fatalf("期望错误 class=%s，但调用成功", want)
	}
	pe, ok := AsError(err)
	if !ok {
		t.Fatalf("期望 *Error(class=%s)，得到非平台错误: %v", want, err)
	}
	logCase(t, "输入错误 → 判定 class=%q reason=%q（期望 %q）", pe.Class, pe.Reason, want)
	if pe.Class != want {
		t.Fatalf("错误类别不符: got=%s want=%s (reason=%s)", pe.Class, want, pe.Reason)
	}
}

// assertOK 断言调用成功并打印判定依据。
func assertOK(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		pe, _ := AsError(err)
		t.Fatalf("%s 应成功，却失败: class=%s reason=%s", what, pclass(pe), preason(pe))
	}
	logCase(t, "%s → 成功（判定依据：error == nil）", what)
}

func pclass(pe *Error) ErrorClass {
	if pe == nil {
		return ""
	}
	return pe.Class
}

func preason(pe *Error) string {
	if pe == nil {
		return ""
	}
	return pe.Reason
}
