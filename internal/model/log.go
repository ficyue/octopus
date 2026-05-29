package model

// AttemptStatus 尝试状态
type AttemptStatus string

const (
	AttemptSuccess      AttemptStatus = "success"       // 转发成功
	AttemptFailed       AttemptStatus = "failed"        // 转发失败
	AttemptCircuitBreak AttemptStatus = "circuit_break" // 熔断跳过
	AttemptSkipped      AttemptStatus = "skipped"       // 其他原因跳过（禁用、无Key、类型不兼容等）
)

// ChannelAttempt 记录单次渠道尝试的决策和结果
type ChannelAttempt struct {
	ChannelID    int           `json:"channel_id"`
	ChannelKeyID int           `json:"channel_key_id,omitempty"`
	ChannelName  string        `json:"channel_name"`
	ModelName    string        `json:"model_name"`
	AttemptNum   int           `json:"attempt_num"`
	Status       AttemptStatus `json:"status"`
	Duration     int           `json:"duration"`
	Sticky       bool          `json:"sticky,omitempty"`
	Msg          string        `json:"msg,omitempty"`
}

// CostItem 费用明细条目
type CostItem struct {
	ItemCode string  `json:"itemCode"`
	Quantity int64   `json:"quantity"`
	Price    float64 `json:"price"`
	Subtotal float64 `json:"subtotal"`
}

// RelayLog 请求日志，字段与 axonhub usage_log 对齐，保留 octopus 特有的 attempts/ftut/useTime 等。
type RelayLog struct {
	ID                int64            `json:"id" gorm:"primaryKey;autoIncrement:false"` // Snowflake ID
	Time              int64            `json:"time"`                                     // 时间戳（秒）
	RequestModelName  string           `json:"request_model_name"`                       // 请求模型名称
	RequestAPIKeyName string           `json:"request_api_key_name"`                     // 请求使用的 API Key 名称
	ChannelId         int              `json:"channel"`                                  // 实际使用的渠道ID
	ChannelName       string           `json:"channel_name"`                             // 渠道名称
	ActualModelName   string           `json:"actual_model_name"`                        // 实际使用模型名称

	// Token 用量（与 axonhub usage_log 对齐）
	PromptTokens            int64 `json:"prompt_tokens"`
	CompletionTokens        int64 `json:"completion_tokens"`
	TotalTokens             int64 `json:"total_tokens"`
	PromptAudioTokens       int64 `json:"prompt_audio_tokens"`
	PromptCachedTokens      int64 `json:"prompt_cached_tokens"`
	PromptWriteCachedTokens int64 `json:"prompt_write_cached_tokens"`
	PromptWriteCached5m     int64 `json:"prompt_write_cached_5m"`
	PromptWriteCached1h     int64 `json:"prompt_write_cached_1h"`
	CompletionAudioTokens   int64 `json:"completion_audio_tokens"`
	CompletionReasonTokens  int64 `json:"completion_reason_tokens"`
	CompletionAcceptedPred  int64 `json:"completion_accepted_pred"`
	CompletionRejectedPred  int64 `json:"completion_rejected_pred"`

	// 费用
	TotalCost float64     `json:"total_cost"`
	CostItems []CostItem  `json:"cost_items" gorm:"serializer:json"`

	// octopus 特有字段
	Ftut            int              `json:"ftut"`                                // 首字时间(毫秒)
	UseTime         int              `json:"use_time"`                            // 总用时(毫秒)
	RequestContent  string           `json:"request_content"`                     // 请求内容
	ResponseContent string           `json:"response_content"`                    // 响应内容
	Error           string           `json:"error"`                               // 错误信息
	Attempts        []ChannelAttempt `json:"attempts" gorm:"serializer:json"`     // 所有尝试记录
	TotalAttempts   int              `json:"total_attempts"`                      // 总尝试次数
}
