package faas

import (
	"encoding/json"
	"fmt"
)

// ProtocolVersion 标识运行时与平台之间调用协议的代际。
type ProtocolVersion string

const (
	// ProtoV1 旧版协议：无 timeoutMs / idempotencyKey 字段，
	// 缺失字段取平台规定的确定默认值（见 decodeV1Defaults）。
	ProtoV1 ProtocolVersion = "1.0"
	// ProtoV2 当前协议：支持显式超时、幂等键与选项透传。
	ProtoV2 ProtocolVersion = "2.0"
)

// v2AllowedOptions 是协议 v2 显式声明过的选项白名单。
// 任何未知选项都必须被明确拒绝，而不是静默忽略。
var v2AllowedOptions = map[string]bool{
	// trace=on|off：是否在响应中回显判定依据，默认 off。
	"trace": true,
}

// RequestOp 标识规范化请求的操作类型。
type RequestOp string

const (
	// OpInvoke 同步调用（默认）。
	OpInvoke RequestOp = "invoke"
	// OpTaskSubmit 异步任务提交。
	OpTaskSubmit RequestOp = "task.submit"
	// OpTaskQuery 异步任务结果查询。
	OpTaskQuery RequestOp = "task.query"
	// OpTaskCancel 异步任务撤销。
	OpTaskCancel RequestOp = "task.cancel"
)

// InvokeRequest 是平台内部统一的规范化调用请求（与协议版本解耦）。
type InvokeRequest struct {
	Op             RequestOp // 为空视为 OpInvoke
	RoutePath      string    // 经由路由调用时填写
	FunctionName   string    // 直接指定函数时填写
	VersionPin     int       // >0 时钉选版本
	Payload        []byte    // 函数入参
	IdempotencyKey string    // 为空则不做去重
	CallTimeoutMS  int       // 0 表示沿用版本配置超时；>0 时不得超过版本配置
	// TaskID 仅 OpTaskQuery / OpTaskCancel 使用。
	TaskID string
	// Options 仅允许协议声明过的键；未知键必须被明确拒绝。
	Options map[string]string
}

// InvokeResponse 是规范化调用结果。
type InvokeResponse struct {
	FunctionName string
	Version      int
	Attempts     int
	InstanceID   string
	Payload      []byte
}

// wireHeader 用于在分发前识别协议代际。
type wireHeader struct {
	Protocol string `json:"protocol"`
}

// wireEnvelopeV1 / wireEnvelopeV2 是各代协议的线上结构。
type wireEnvelopeV1 struct {
	Protocol string `json:"protocol"`
	Route    string `json:"route"`
	Function string `json:"function,omitempty"`
	Payload  []byte `json:"payload"`
}

type wireEnvelopeV2 struct {
	Protocol  string            `json:"protocol"`
	Op        string            `json:"op,omitempty"`
	Route     string            `json:"route,omitempty"`
	Function  string            `json:"function,omitempty"`
	Version   int               `json:"version,omitempty"`
	Payload   []byte            `json:"payload"`
	TimeoutMS int               `json:"timeoutMs,omitempty"`
	IdemKey   string            `json:"idempotencyKey,omitempty"`
	TaskID    string            `json:"taskId,omitempty"`
	Options   map[string]string `json:"options,omitempty"`
}

// DecodeRequest 按协议版本解码外部请求并规范化为内部请求。
// 旧版缺失字段采用确定默认值；不支持的协议版本 / 未知选项被拒绝。
func DecodeRequest(raw []byte) (ProtocolVersion, *InvokeRequest, error) {
	var head wireHeader
	if err := json.Unmarshal(raw, &head); err != nil {
		return "", nil, wrapError(ErrClassInvalid, "request is not valid JSON", err)
	}
	switch ProtocolVersion(head.Protocol) {
	case ProtoV1:
		v, err := decodeV1(raw)
		return ProtoV1, v, err
	case ProtoV2:
		v, err := decodeV2(raw)
		return ProtoV2, v, err
	case "":
		return "", nil, newError(ErrClassInvalid, "missing required field: protocol")
	default:
		return "", nil, newError(ErrClassInvalid,
			"unsupported protocol version: "+head.Protocol)
	}
}

// decodeV1 解析旧版协议。旧协议没有超时、幂等键与选项字段，
// 这里的归一化默认值是唯一权威取值，杜绝"缺失字段结论不确定"。
func decodeV1(raw []byte) (*InvokeRequest, error) {
	var env wireEnvelopeV1
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, wrapError(ErrClassInvalid, "malformed v1 envelope", err)
	}
	if env.Route == "" && env.Function == "" {
		return nil, newError(ErrClassInvalid, "v1 request requires route or function")
	}
	if env.Route != "" && env.Function != "" {
		return nil, newError(ErrClassInvalid, "route and function are mutually exclusive")
	}
	return &InvokeRequest{
		RoutePath:      env.Route,
		FunctionName:   env.Function,
		VersionPin:     0, // v1 永远跟随活动版本
		Payload:        env.Payload,
		IdempotencyKey: "", // 确定默认：不去重
		CallTimeoutMS:  0,  // 确定默认：沿用版本配置超时
		Options:        nil,
	}, nil
}

// decodeV2 解析当前协议，并对字段做严格校验。
func decodeV2(raw []byte) (*InvokeRequest, error) {
	var env wireEnvelopeV2
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, wrapError(ErrClassInvalid, "malformed v2 envelope", err)
	}
	op := RequestOp(env.Op)
	if op == "" {
		op = OpInvoke
	}
	switch op {
	case OpInvoke, OpTaskSubmit:
		return decodeV2Invoke(op, &env)
	case OpTaskQuery, OpTaskCancel:
		return decodeV2TaskOp(op, &env)
	default:
		return nil, newError(ErrClassInvalid,
			fmt.Sprintf("unsupported op %q (not silently downgraded to sync invoke)", env.Op))
	}
}

// decodeV2Invoke 校验同步调用 / 异步提交共用的调用字段。
func decodeV2Invoke(op RequestOp, env *wireEnvelopeV2) (*InvokeRequest, error) {
	if env.TaskID != "" {
		return nil, newError(ErrClassInvalid, "taskId is only allowed for task.query/task.cancel")
	}
	if env.Route == "" && env.Function == "" {
		return nil, newError(ErrClassInvalid, "v2 request requires route or function")
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
	if err := validateV2Options(env.Options); err != nil {
		return nil, err
	}
	return &InvokeRequest{
		Op:             op,
		RoutePath:      env.Route,
		FunctionName:   env.Function,
		VersionPin:     env.Version,
		Payload:        env.Payload,
		IdempotencyKey: env.IdemKey,
		CallTimeoutMS:  env.TimeoutMS,
		Options:        env.Options,
	}, nil
}

// decodeV2TaskOp 校验任务查询 / 撤销：只允许携带 taskId，
// 任何调用字段的混入都是非法字段组合，必须明确拒绝。
func decodeV2TaskOp(op RequestOp, env *wireEnvelopeV2) (*InvokeRequest, error) {
	if env.TaskID == "" {
		return nil, newError(ErrClassInvalid, string(op)+" requires taskId")
	}
	if env.Route != "" || env.Function != "" || env.Version != 0 ||
		env.Payload != nil || env.TimeoutMS != 0 || env.IdemKey != "" || len(env.Options) > 0 {
		return nil, newError(ErrClassInvalid,
			string(op)+" only accepts taskId; invoke fields are not allowed in the same request")
	}
	return &InvokeRequest{Op: op, TaskID: env.TaskID}, nil
}

// validateV2Options 校验选项白名单：未知选项 / 非法值明确拒绝。
func validateV2Options(opts map[string]string) error {
	for k, v := range opts {
		if !v2AllowedOptions[k] {
			return newError(ErrClassInvalid,
				fmt.Sprintf("unsupported option %q (not silently ignored)", k))
		}
		if k == "trace" && v != "on" && v != "off" {
			return newError(ErrClassInvalid,
				fmt.Sprintf("invalid value %q for option trace", v))
		}
	}
	return nil
}

// wireResponseV1 是旧客户端可见的最小确定结构（不暴露 attempts 等新字段）。
type wireResponseV1 struct {
	Protocol    string `json:"protocol"`
	OK          bool   `json:"ok"`
	Function    string `json:"function,omitempty"`
	Version     int    `json:"version,omitempty"`
	Payload     []byte `json:"payload,omitempty"`
	ErrorClass  string `json:"errorClass,omitempty"`
	ErrorReason string `json:"errorReason,omitempty"`
}

// wireResponseV2 在 v1 基础上增加尝试次数等诊断字段。
type wireResponseV2 struct {
	Protocol    string `json:"protocol"`
	OK          bool   `json:"ok"`
	Function    string `json:"function,omitempty"`
	Version     int    `json:"version,omitempty"`
	Attempts    int    `json:"attempts,omitempty"`
	InstanceID  string `json:"instanceId,omitempty"`
	Payload     []byte `json:"payload,omitempty"`
	ErrorClass  string `json:"errorClass,omitempty"`
	ErrorReason string `json:"errorReason,omitempty"`
}

// EncodeResponse 把内部结果按目标协议版本编码；归一化保证旧客户端结论确定。
func EncodeResponse(version ProtocolVersion, resp *InvokeResponse, callErr error) ([]byte, error) {
	var class, reason string
	if callErr != nil {
		if pe, ok := AsError(callErr); ok {
			class = string(pe.Class)
			reason = pe.Reason
		} else {
			class = string(ErrClassPlatform)
			reason = callErr.Error()
		}
	}
	switch version {
	case ProtoV1:
		out := wireResponseV1{Protocol: string(ProtoV1), OK: callErr == nil,
			ErrorClass: class, ErrorReason: reason}
		if resp != nil {
			out.Function = resp.FunctionName
			out.Version = resp.Version
			// 失败时不携带函数载荷，但成功时回显。
			if callErr == nil {
				out.Payload = resp.Payload
			}
		}
		return json.Marshal(out)
	case ProtoV2, "":
		out := wireResponseV2{Protocol: string(ProtoV2), OK: callErr == nil,
			ErrorClass: class, ErrorReason: reason}
		if resp != nil {
			out.Function = resp.FunctionName
			out.Version = resp.Version
			out.Attempts = resp.Attempts // 成功与失败都暴露尝试次数，判定可解释
			out.InstanceID = resp.InstanceID
			if callErr == nil {
				out.Payload = resp.Payload
			}
		}
		return json.Marshal(out)
	default:
		return nil, newError(ErrClassInvalid, "unsupported protocol version: "+string(version))
	}
}

// wireTaskResponseV2 是异步任务提交/查询/撤销的线上响应结构。
type wireTaskResponseV2 struct {
	Protocol    string `json:"protocol"`
	OK          bool   `json:"ok"`
	TaskID      string `json:"taskId,omitempty"`
	TaskStatus  string `json:"taskStatus,omitempty"`
	Function    string `json:"function,omitempty"`
	Version     int    `json:"version,omitempty"`
	Attempts    int    `json:"attempts,omitempty"`
	Payload     []byte `json:"payload,omitempty"`
	ErrorClass  string `json:"errorClass,omitempty"`
	ErrorReason string `json:"errorReason,omitempty"`
}

// EncodeTaskResponse 把任务快照编码为线上响应。
// 异步任务是 v2 能力；v1 报文没有 op 字段，解码阶段不可能产生任务操作，
// 这里对其他版本做防御性拒绝。
// ok 表示本次操作（提交/查询/撤销）是否成功；任务自身的终态结论
// 由 taskStatus 与 errorClass/errorReason 透出，彼此可区分。
func EncodeTaskResponse(version ProtocolVersion, snap *TaskSnapshot, callErr error) ([]byte, error) {
	if version != ProtoV2 && version != "" {
		return nil, newError(ErrClassInvalid, "async tasks require protocol "+string(ProtoV2))
	}
	out := wireTaskResponseV2{Protocol: string(ProtoV2), OK: callErr == nil}
	if callErr != nil {
		if pe, ok := AsError(callErr); ok {
			out.ErrorClass = string(pe.Class)
			out.ErrorReason = pe.Reason
		} else {
			out.ErrorClass = string(ErrClassPlatform)
			out.ErrorReason = callErr.Error()
		}
	}
	if snap != nil {
		out.TaskID = snap.TaskID
		out.TaskStatus = string(snap.Status)
		out.Function = snap.FunctionName
		out.Version = snap.Version
		out.Attempts = snap.Attempts
		if snap.Status == TaskSucceeded {
			out.Payload = snap.Payload
		}
		if snap.ErrorClass != "" {
			out.ErrorClass = snap.ErrorClass
			out.ErrorReason = snap.ErrorReason
		}
	}
	return json.Marshal(out)
}
