package model

// StatsChannelPeriod 渠道按周期聚合统计
type StatsChannelPeriod struct {
	ChannelID   int     `json:"channel_id"`
	ChannelName string  `json:"channel_name"`
	InputTokens int64   `json:"input_token"`
	OutputTokens int64  `json:"output_token"`
	TotalTokens int64   `json:"total_token"`
	InputCost   float64 `json:"input_cost"`
	OutputCost  float64 `json:"output_cost"`
	TotalCost   float64 `json:"total_cost"`
	Requests    int64   `json:"requests"`
	Successes   int64   `json:"successes"`
	Failures    int64   `json:"failures"`
	TotalMs     int64   `json:"total_ms"`     // 总耗时(毫秒)
	AvgLatency  int64   `json:"avg_latency"`  // 平均延迟(毫秒)
	TokensPerSec float64 `json:"tokens_per_sec"` // 输出 token 速度 (输出 token / 总秒数)
}
