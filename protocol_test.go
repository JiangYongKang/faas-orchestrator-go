package faas

import (
	"encoding/json"
	"testing"
)

func TestDecode_V1DeterministicDefaults(t *testing.T) {
	raw := []byte(`{"protocol":"1.0","route":"/r","payload":"aGVsbG8="}`)
	ver, req, err := DecodeRequest(raw)
	assertOK(t, err, "解码 v1 报文")
	if ver != ProtoV1 {
		t.Fatalf("协议版本应为 1.0, got %s", ver)
	}
	if req.VersionPin != 0 || req.IdempotencyKey != "" || req.CallTimeoutMS != 0 || req.Options != nil {
		t.Fatalf("v1 缺失字段必须取确定零值: %+v", req)
	}
	logCase(t, "输入 v1 缺字段报文 → pin=0 idem=\"\" timeoutMS=0 options=nil，判定依据：默认值唯一权威")
}

func TestDecode_Rejections(t *testing.T) {
	cases := []struct {
		name  string
		raw   string
		want  ErrorClass
		input string
	}{
		{"非JSON", `{not-json`, ErrClassInvalid, "malformed json"},
		{"缺协议字段", `{"route":"/r"}`, ErrClassInvalid, "protocol missing"},
		{"未知协议版本", `{"protocol":"9.9","route":"/r"}`, ErrClassInvalid, "protocol=9.9"},
		{"v2 未知选项", `{"protocol":"2.0","route":"/r","options":{"mystery":"1"}}`, ErrClassInvalid, "option mystery"},
		{"v2 trace 非法值", `{"protocol":"2.0","route":"/r","options":{"trace":"yes"}}`, ErrClassInvalid, "trace=yes"},
		{"v2 route与version并存", `{"protocol":"2.0","route":"/r","version":2}`, ErrClassInvalid, "route+version"},
		{"v2 route与function并存", `{"protocol":"2.0","route":"/r","function":"f"}`, ErrClassInvalid, "route+function"},
		{"v2 负超时", `{"protocol":"2.0","function":"f","timeoutMs":-5}`, ErrClassInvalid, "timeoutMs=-5"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			logCase(t, "输入 %s", c.input)
			_, _, err := DecodeRequest([]byte(c.raw))
			assertClass(t, err, c.want)
		})
	}
}

func TestDecode_V2Accepted(t *testing.T) {
	raw := []byte(`{"protocol":"2.0","function":"fn","version":3,"timeoutMs":50,"idempotencyKey":"k1","options":{"trace":"on"}}`)
	ver, req, err := DecodeRequest(raw)
	assertOK(t, err, "解码合法 v2 报文")
	if ver != ProtoV2 || req.VersionPin != 3 || req.CallTimeoutMS != 50 || req.IdempotencyKey != "k1" {
		t.Fatalf("v2 字段解析错误: %+v", req)
	}
	logCase(t, "输入完整 v2 报文 → 全部字段正确归一化")
}

func TestEncode_StableAcrossVersions(t *testing.T) {
	resp := &InvokeResponse{FunctionName: "fn", Version: 2, Attempts: 3, InstanceID: "inst-9", Payload: []byte("ok")}
	err := newError(ErrClassTimeout, "boom")

	v1, e1 := EncodeResponse(ProtoV1, resp, err)
	assertOK(t, e1, "编码 v1 错误响应")
	var m1 map[string]any
	_ = json.Unmarshal(v1, &m1)
	if _, has := m1["attempts"]; has {
		t.Fatalf("v1 响应不得出现新字段 attempts，保证旧客户端结论确定: %s", v1)
	}
	if m1["errorClass"] != string(ErrClassTimeout) {
		t.Fatalf("v1 错误类别必须可见: %s", v1)
	}
	logCase(t, "输入 v2 结果含 attempts → v1 编码剔除新字段且保留 errorClass=timeout")

	v2, e2 := EncodeResponse(ProtoV2, resp, nil)
	assertOK(t, e2, "编码 v2 成功响应")
	var m2 map[string]any
	_ = json.Unmarshal(v2, &m2)
	if m2["attempts"].(float64) != 3 || m2["instanceId"] != "inst-9" {
		t.Fatalf("v2 应携带 attempts/instanceId: %s", v2)
	}
	logCase(t, "输入成功结果 → v2 编码携带 attempts=3 instanceId=inst-9")
}
