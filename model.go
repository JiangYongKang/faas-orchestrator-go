package faas

import "time"

// VersionState 描述一个已发布版本的可用性。
type VersionState string

const (
	VersionActive   VersionState = "active"   // 可接受调用
	VersionDisabled VersionState = "disabled" // 被显式下线，不可调用，可重新启用
	VersionRetired  VersionState = "retired"  // 已废弃，不可再启用
)

// RetryPolicy 描述可重试失败的重试规则。
type RetryPolicy struct {
	// MaxAttempts 最大尝试次数（含首次），<=1 表示不重试。
	MaxAttempts int
	// Backoff 相邻两次尝试之间的固定退避间隔。
	Backoff time.Duration
}

// DefaultRetryPolicy 默认不重试，保证语义显式。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1, Backoff: 0}
}

// VersionConfig 是一次发布携带的不可变配置。
type VersionConfig struct {
	// HandlerName 指定本地运行时中注册的函数处理器名。
	HandlerName string
	// Timeout 单次执行的硬超时。
	Timeout time.Duration
	// MaxConcurrency 该版本允许的最大并发在执调用数。
	MaxConcurrency int
	// Retry 调用级别的失败重试策略。
	Retry RetryPolicy
}

// FunctionVersion 是一次发布产生的不可变版本记录（状态可变，配置不可变）。
type FunctionVersion struct {
	FunctionName string
	Version      int
	Config       VersionConfig
	State        VersionState
	PublishedAt  time.Time
}

// Clone 返回深拷贝，避免外部读写内部状态。
func (v *FunctionVersion) Clone() *FunctionVersion {
	cp := *v
	return &cp
}

// Function 聚合一个函数的全部版本与历史。
type Function struct {
	Name          string
	Versions      map[int]*FunctionVersion // 版本号 -> 版本记录
	History       []PublishRecord          // 按发布顺序排列
	ActiveVersion int                      // 当前指向的版本；0 表示无
	nextVersion   int
}

// PublishRecord 记录一次发布或回滚事件，用于可解释的历史追溯。
type PublishRecord struct {
	Kind        string // "publish" | "rollback" | "disable" | "enable" | "retire"
	Version     int    // 事件关联的目标版本
	FromVersion int    // 切换前的活动版本（无则 0）
	OccurredAt  time.Time
	Note        string
}

// Route 表示一条到函数版本的路由绑定。
type Route struct {
	Path         string
	FunctionName string
	// VersionPin 为 0 时跟随函数当前活动版本；>0 时固定指向该版本。
	VersionPin int
}
