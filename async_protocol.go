package faas

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// 异步链路仅在 v2 协议上开放：v1 客户端必须显式升级协议，
// 平台绝不把异步请求静默降级为同步执行。
//
// 报文约定（在 v2 同步报文之上显式增加 mode=async）：
//
//	提交： {"protocol":"2.0","mode":"async","route":"/p" | "function":"f",
//	        "version":n?,"payload":...,"timeoutMs":n?,"idempotencyKey":"...",
//	        "options":{...}?}
//	查询： {"protocol":"2.0","mode":"async","op":"query","taskId":"..."}
//	撤销： {"protocol":"2.0","mode":"async","op":"cancel","taskId":"..."}
//
// 任何未知顶层字段 / 未知选项都被明确拒绝（严格解码），绝不静默忽略。

const asyncMode = "async"

// wireAsyncSubmit 是异步提交报文（v2 + mode=async）。
type wireAsyncSubmit struct {
	Protocol  string            `json:"protocol"`
	Mode      string            `json:"mode"`
	Route     string            `json:"route,omitempty"`
	Function  string            `json:"function,omitempty"`
	Version   int               `json:"version,omitempty"`
	Payload   []byte            `json:"payload,omitempty"`
	TimeoutMS int               `json:"timeoutMs,omitempty"`
	IdemKey   string            `json:"idempotencyKey,omitempty"`
	Options   map[string]string `json:"options,omitempty"`
}

// wireAsyncAck 是提交成功后立即返回的任务凭证报文。
type wireAsyncAck struct {
	Protocol string `json:"protocol"`
	Mode     string `json:"mode"`
	OK       bool   `json:"ok"`
	TaskID   string `json:"taskId,omitempty"`
	State    string `json:"state,omitempty"`
	// ErrorClass / ErrorReason 在受理失败时填充。
	ErrorClass  string `json:"errorClass,omitempty"`
	ErrorReason string `json:"errorReason,omitempty"`
}

// wireAsyncQuery 是结果查询 / 撤销请求报文。
type wireAsyncQuery struct {
	Protocol string `json:"protocol"`
	Mode     string `json:"mode"`
	Op       string `json:"op"`
	TaskID   string `json:"taskId"`
}

// wireAsyncStatus 是查询 / 撤销响应报文。
type wireAsyncStatus struct {
	Protocol    string `json:"protocol"`
	Mode        string `json:"mode"`
	Op          string `json:"op"`
	OK          bool   `json:"ok"`
	TaskID      string `json:"taskId,omitempty"`
	State       string `json:"state,omitempty"`
	Function    string `json:"function,omitempty"`
	Version     int    `json:"version,omitempty"`
	Attempts    int    `json:"attempts,omitempty"`
	InstanceID  string `json:"instanceId,omitempty"`
	Payload     []byte `json:"payload,omitempty"`
	ErrorClass  string `json:"errorClass,omitempty"`
	ErrorReason string `json:"errorReason,omitempty"`
}

// strictUnmarshal 在普通解码之外拒绝任何未知字段，杜绝静默忽略。
func strictUnmarshal(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return wrapError(ErrClassInvalid, "malformed async envelope", err)
	}
	if dec.More() {
		return newError(ErrClassInvalid, "malformed async envelope: unexpected trailing JSON value")
	}
	return nil
}

// DecodeAsyncSubmit 解码并严格校验异步提交报文。
func DecodeAsyncSubmit(raw []byte) (*InvokeRequest, error) {
	var env wireAsyncSubmit
	if err := strictUnmarshal(raw, &env); err != nil {
		return nil, err
	}
	if env.Protocol != string(ProtoV2) {
		if env.Protocol == string(ProtoV1) || env.Protocol == "" {
			return nil, newError(ErrClassInvalid,
				"async invocation requires protocol \"2.0\" with \"mode\":\"async\"; "+
					"it is never silently downgraded to synchronous execution")
		}
		return nil, newError(ErrClassInvalid, "unsupported protocol version: "+env.Protocol)
	}
	if env.Mode != asyncMode {
		return nil, newError(ErrClassInvalid,
			fmt.Sprintf("async submit requires mode=%q, got %q (sync endpoint must not be used)",
				asyncMode, env.Mode))
	}
	if env.Route == "" && env.Function == "" {
		return nil, newError(ErrClassInvalid, "async submit requires route or function")
	}
	if env.Route != "" && env.Function != "" {
		return nil, newError(ErrClassInvalid, "route and function are mutually exclusive")
	}
	if env.Route != "" && env.Version != 0 {
		return nil, newError(ErrClassInvalid, "version pin is only allowed for direct function calls")
	}
	if env.Version < 0 {
		return nil, newError(ErrClassInvalid, "version must be >= 0")
	}
	if env.TimeoutMS < 0 {
		return nil, newError(ErrClassInvalid, "timeoutMs must be >= 0")
	}
	for k, v := range env.Options {
		if !v2AllowedOptions[k] {
			return nil, newError(ErrClassInvalid,
				fmt.Sprintf("unsupported option %q (not silently ignored)", k))
		}
		if k == "trace" && v != "on" && v != "off" {
			return nil, newError(ErrClassInvalid,
				fmt.Sprintf("invalid value %q for option trace", v))
		}
	}
	return &InvokeRequest{
		RoutePath:      env.Route,
		FunctionName:   env.Function,
		VersionPin:     env.Version,
		Payload:        append([]byte(nil), env.Payload...),
		IdempotencyKey: env.IdemKey,
		CallTimeoutMS:  env.TimeoutMS,
		Options:        env.Options,
	}, nil
}

// DecodeAsyncOp 解码并严格校验查询 / 撤销报文。
func DecodeAsyncOp(raw []byte) (op string, taskID string, err error) {
	var env wireAsyncQuery
	if err := strictUnmarshal(raw, &env); err != nil {
		return "", "", err
	}
	if env.Protocol != string(ProtoV2) {
		return "", "", newError(ErrClassInvalid,
			"async query/cancel requires protocol \"2.0\" with \"mode\":\"async\"")
	}
	if env.Mode != asyncMode {
		return "", "", newError(ErrClassInvalid,
			fmt.Sprintf("async op requires mode=%q, got %q", asyncMode, env.Mode))
	}
	switch env.Op {
	case "query", "cancel":
	default:
		return "", "", newError(ErrClassInvalid,
			fmt.Sprintf("unsupported async op %q (allowed: query, cancel)", env.Op))
	}
	if env.TaskID == "" {
		return "", "", newError(ErrClassInvalid, "taskId must not be empty")
	}
	return env.Op, env.TaskID, nil
}

// classifyCallErr 提取平台错误的类别与原因；未分类错误归为 platform_error。
func classifyCallErr(callErr error) (ErrorClass, string) {
	if callErr == nil {
		return "", ""
	}
	if pe, ok := AsError(callErr); ok {
		return pe.Class, pe.Reason
	}
	return ErrClassPlatform, callErr.Error()
}

// EncodeAsyncAck 编码提交受理结果。
func EncodeAsyncAck(taskID string, state TaskState, callErr error) ([]byte, error) {
	out := wireAsyncAck{Protocol: string(ProtoV2), Mode: asyncMode, OK: callErr == nil}
	if taskID != "" {
		out.TaskID = taskID
	}
	if state != "" {
		out.State = string(state)
	}
	if callErr != nil {
		ec, er := classifyCallErr(callErr)
		out.ErrorClass = string(ec)
		out.ErrorReason = er
	}
	return json.Marshal(out)
}

// EncodeAsyncStatus 编码查询 / 撤销结果。
func EncodeAsyncStatus(op string, snap *TaskSnapshot, callErr error) ([]byte, error) {
	out := wireAsyncStatus{
		Protocol: string(ProtoV2),
		Mode:     asyncMode,
		Op:       op,
		OK:       callErr == nil,
	}
	if callErr != nil {
		ec, er := classifyCallErr(callErr)
		out.ErrorClass = string(ec)
		out.ErrorReason = er
	}
	if snap != nil {
		out.TaskID = snap.ID
		out.State = string(snap.State)
		out.Function = snap.FunctionName
		out.Version = snap.Version
		out.Attempts = snap.Attempts
		out.InstanceID = snap.InstanceID
		if snap.State == TaskSucceeded {
			// 仅成功终态回显函数载荷；失败终态的结论由 errorClass/errorReason 承载。
			out.Payload = append([]byte(nil), snap.Payload...)
		}
		if snap.ErrorClass != "" {
			out.ErrorClass = string(snap.ErrorClass)
			out.ErrorReason = snap.ErrorReason
		}
	}
	return json.Marshal(out)
}
