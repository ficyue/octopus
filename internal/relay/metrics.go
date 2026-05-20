package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/price"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/utils/log"
)

// RelayMetrics 负责最终的日志收集与持久化
type RelayMetrics struct {
	APIKeyID     int
	RequestModel string
	StartTime    time.Time

	// 首 Token 时间
	FirstTokenTime time.Time

	// 请求和响应内容
	InternalRequest  *transformerModel.InternalLLMRequest
	InternalResponse *transformerModel.InternalLLMResponse

	// 统计指标
	ActualModel string
	Stats       model.StatsMetrics

	// 参数覆盖
	ParamOverride string

	// 透传模式原始数据，用于日志记录
	PassthroughRequest  []byte
	PassthroughResponse []byte
}

func NewRelayMetrics(apiKeyID int, requestModel string, req *transformerModel.InternalLLMRequest) *RelayMetrics {
	return &RelayMetrics{
		APIKeyID:        apiKeyID,
		RequestModel:    requestModel,
		StartTime:       time.Now(),
		InternalRequest: req,
	}
}

func (m *RelayMetrics) SetActualModel(model string) {
	m.ActualModel = model
}

func (m *RelayMetrics) SetPassthroughBody(request, response []byte) {
	m.PassthroughRequest = request
	m.PassthroughResponse = response
}

func (m *RelayMetrics) SetFirstTokenTime(t time.Time) {
	m.FirstTokenTime = t
}

func (m *RelayMetrics) SetInternalResponse(resp *transformerModel.InternalLLMResponse, actualModel string) {
	m.InternalResponse = resp
	m.ActualModel = actualModel

	if resp == nil || resp.Usage == nil {
		return
	}

	usage := resp.Usage
	m.Stats.InputToken = usage.PromptTokens
	m.Stats.OutputToken = usage.CompletionTokens

	if usage.PromptTokensDetails == nil {
		usage.PromptTokensDetails = &transformerModel.PromptTokensDetails{
			CachedTokens: 0,
		}
	}
	m.Stats.CachedTokens = usage.PromptTokensDetails.CachedTokens

	modelPrice := price.GetLLMPrice(actualModel)
	if modelPrice == nil {
		return
	}
	if usage.AnthropicUsage {
		m.Stats.InputCost = (float64(usage.PromptTokensDetails.CachedTokens)*modelPrice.CacheRead +
			float64(usage.PromptTokens)*modelPrice.Input +
			float64(usage.CacheCreationInputTokens)*modelPrice.CacheWrite) * 1e-6
	} else {
		m.Stats.InputCost = (float64(usage.PromptTokensDetails.CachedTokens)*modelPrice.CacheRead + float64(usage.PromptTokens-usage.PromptTokensDetails.CachedTokens)*modelPrice.Input) * 1e-6
	}
	m.Stats.OutputCost = float64(usage.CompletionTokens) * modelPrice.Output * 1e-6
}

func (m *RelayMetrics) Save(ctx context.Context, success bool, err error, attempts []model.ChannelAttempt) {
	duration := time.Since(m.StartTime)

	globalStats := model.StatsMetrics{
		WaitTime:     duration.Milliseconds(),
		InputToken:   m.Stats.InputToken,
		OutputToken:  m.Stats.OutputToken,
		CachedTokens: m.Stats.CachedTokens,
		InputCost:    m.Stats.InputCost,
		OutputCost:   m.Stats.OutputCost,
	}
	if success {
		globalStats.RequestSuccess = 1
	} else {
		globalStats.RequestFailed = 1
	}

	channelID, channelName := finalChannel(attempts)
	op.StatsTotalUpdate(globalStats)
	op.StatsHourlyUpdate(globalStats)
	op.StatsDailyUpdate(context.Background(), globalStats)
	op.StatsAPIKeyUpdate(m.APIKeyID, globalStats)
	op.StatsChannelUpdate(channelID, globalStats)

	log.Infof("relay complete: model=%s, channel=%d(%s), success=%t, duration=%dms, input_token=%d, output_token=%d, input_cost=%f, output_cost=%f, total_cost=%f, attempts=%d",
		m.RequestModel, channelID, channelName, success, duration.Milliseconds(),
		m.Stats.InputToken, m.Stats.OutputToken,
		m.Stats.InputCost, m.Stats.OutputCost, m.Stats.InputCost+m.Stats.OutputCost,
		len(attempts))

	m.saveLog(ctx, err, duration, attempts, channelID, channelName)
}

func finalChannel(attempts []model.ChannelAttempt) (int, string) {
	var lastID int
	var lastName string
	for i := len(attempts) - 1; i >= 0; i-- {
		a := attempts[i]
		if a.Status == model.AttemptSuccess {
			return a.ChannelID, a.ChannelName
		}
		if a.Status == model.AttemptFailed && lastID == 0 {
			lastID = a.ChannelID
			lastName = a.ChannelName
		}
	}
	return lastID, lastName
}

func (m *RelayMetrics) saveLog(ctx context.Context, err error, duration time.Duration, attempts []model.ChannelAttempt, channelID int, channelName string) {
	actualModel := m.ActualModel
	if actualModel == "" {
		actualModel = m.RequestModel
	}

	relayLog := model.RelayLog{
		Time:             m.StartTime.Unix(),
		RequestModelName: m.RequestModel,
		ChannelName:      channelName,
		ChannelId:        channelID,
		ActualModelName:  actualModel,
		UseTime:          int(duration.Milliseconds()),
		Attempts:         attempts,
		TotalAttempts:    len(attempts),
	}

	if apiKey, getErr := op.APIKeyGet(m.APIKeyID, ctx); getErr == nil {
		relayLog.RequestAPIKeyName = apiKey.Name
	}

	// 首字时间
	if !m.FirstTokenTime.IsZero() {
		relayLog.Ftut = int(m.FirstTokenTime.Sub(m.StartTime).Milliseconds())
	}

	// Usage
	if m.InternalResponse != nil && m.InternalResponse.Usage != nil {
		relayLog.InputTokens = int(m.InternalResponse.Usage.PromptTokens)
		relayLog.OutputTokens = int(m.InternalResponse.Usage.CompletionTokens)
		relayLog.CachedTokens = int(m.Stats.CachedTokens)
		relayLog.Cost = m.Stats.InputCost + m.Stats.OutputCost
	}

	// 请求内容：透传模式优先使用原始请求体
	if len(m.PassthroughRequest) > 0 {
		relayLog.RequestContent = string(m.PassthroughRequest)
	} else if m.InternalRequest != nil {
		reqJSON, jsonErr := json.Marshal(m.InternalRequest)
		if jsonErr != nil {
			relayLog.RequestContent = string(reqJSON)
		} else if m.ParamOverride == "" {
			relayLog.RequestContent = string(reqJSON)
		} else {
			var reqMap map[string]any
			if err := json.Unmarshal(reqJSON, &reqMap); err != nil {
				relayLog.RequestContent = string(reqJSON)
			} else {
				var override map[string]any
				if err := json.Unmarshal([]byte(m.ParamOverride), &override); err != nil {
					relayLog.RequestContent = string(reqJSON)
				} else {
					maps.Copy(reqMap, override)
					if finalJSON, err := json.Marshal(reqMap); err != nil {
						relayLog.RequestContent = string(reqJSON)
					} else {
						relayLog.RequestContent = string(finalJSON)
					}
				}
			}
		}
	}

	// 响应内容：透传模式优先使用原始响应体
	if len(m.PassthroughResponse) > 0 {
		// 透传模式使用原始响应体（截断过长的内容）
		maxRespLen := 50000
		if len(m.PassthroughResponse) > maxRespLen {
			relayLog.ResponseContent = string(m.PassthroughResponse[:maxRespLen])
		} else {
			relayLog.ResponseContent = string(m.PassthroughResponse)
		}
	} else if m.InternalResponse != nil {
		respForLog := m.filterResponseForLog(m.InternalResponse)
		if respJSON, jsonErr := json.Marshal(respForLog); jsonErr == nil {
			if m.InternalResponse.Usage != nil && m.InternalResponse.Usage.AnthropicUsage {
				respStr := string(respJSON)
				old := `"usage":{`
				insert := fmt.Sprintf(`"usage":{"cache_creation_input_tokens":%d,`, m.InternalResponse.Usage.CacheCreationInputTokens)
				respJSON = []byte(strings.Replace(respStr, old, insert, 1))
			}
			relayLog.ResponseContent = string(respJSON)
		}
	}

	// 错误信息
	if err != nil {
		relayLog.Error = err.Error()
	}

	if logErr := op.RelayLogAdd(ctx, relayLog); logErr != nil {
		log.Warnf("failed to save relay log: %v", logErr)
	}
}

// ExtractUsageFromRawResponse 从原始响应体中提取 usage 信息，用于透传模式
func (m *RelayMetrics) ExtractUsageFromRawResponse(respBody []byte, isStream bool) {
	if len(respBody) == 0 {
		return
	}

	// extractUsage 从 JSON 数据中提取 usage，兼容多种响应格式
	extractUsage := func(data []byte) {
		preview := string(data)
		if len(preview) > 200 {
			preview = preview[:200]
		}

		// 1. OpenAI Chat Completions 格式：顶层 usage 字段
		var chatUsage struct {
			Usage *transformerModel.Usage `json:"usage"`
		}
		if err := json.Unmarshal(data, &chatUsage); err == nil && chatUsage.Usage != nil {
			cachedTokens := int64(0)
			if chatUsage.Usage.PromptTokensDetails != nil {
				cachedTokens = chatUsage.Usage.PromptTokensDetails.CachedTokens
			}
			log.Debugf("passthrough extractUsage: found chat usage, prompt=%d, completion=%d, cached=%d", chatUsage.Usage.PromptTokens, chatUsage.Usage.CompletionTokens, cachedTokens)
			m.SetInternalResponse(&transformerModel.InternalLLMResponse{Usage: chatUsage.Usage}, m.ActualModel)
			return
		}

		// 2. OpenAI Responses API 流式格式：response.completed 事件中 response.usage
		var respEvent struct {
			Type     string `json:"type"`
			Response struct {
				Usage *struct {
					InputTokens       int64 `json:"input_tokens"`
					OutputTokens      int64 `json:"output_tokens"`
					TotalTokens       int64 `json:"total_tokens"`
					InputTokenDetails struct {
						CachedTokens int64 `json:"cached_tokens"`
					} `json:"input_tokens_details"`
					OutputTokenDetails struct {
						ReasoningTokens int64 `json:"reasoning_tokens"`
					} `json:"output_tokens_details"`
				} `json:"usage"`
			} `json:"response"`
		}
		if err := json.Unmarshal(data, &respEvent); err == nil && respEvent.Response.Usage != nil {
			u := respEvent.Response.Usage
			log.Debugf("passthrough extractUsage: found responses API usage, input=%d, output=%d, cached=%d", u.InputTokens, u.OutputTokens, u.InputTokenDetails.CachedTokens)
			usage := &transformerModel.Usage{
				PromptTokens:     u.InputTokens,
				CompletionTokens: u.OutputTokens,
				TotalTokens:      u.TotalTokens,
			}
			if u.InputTokenDetails.CachedTokens > 0 {
				usage.PromptTokensDetails = &transformerModel.PromptTokensDetails{
					CachedTokens: u.InputTokenDetails.CachedTokens,
				}
			}
			if u.OutputTokenDetails.ReasoningTokens > 0 {
				usage.CompletionTokensDetails = &transformerModel.CompletionTokensDetails{
					ReasoningTokens: u.OutputTokenDetails.ReasoningTokens,
				}
			}
			m.SetInternalResponse(&transformerModel.InternalLLMResponse{Usage: usage}, m.ActualModel)
			return
		}

		log.Debugf("passthrough extractUsage: no usage found in data, preview: %s", preview)
	}

	if isStream {
		// 流式响应：逐行解析 SSE 事件，提取最后一个包含 usage 的 data
		lines := bytes.Split(respBody, []byte("\n"))
		for i := len(lines) - 1; i >= 0; i-- {
			line := bytes.TrimSpace(lines[i])
			if bytes.HasPrefix(line, []byte("data:")) || bytes.HasPrefix(line, []byte("data: ")) {
				payload := bytes.TrimPrefix(line, []byte("data:"))
				payload = bytes.TrimPrefix(payload, []byte(" "))
				if bytes.Equal(payload, []byte("[DONE]")) {
					continue
				}
				extractUsage(payload)
				// 如果已经找到 usage，SetInternalResponse 会设置 InternalResponse，
				// 后续再次调用时 InternalResponse 不为 nil，但 Stats 已经更新过了
				if m.InternalResponse != nil {
					return
				}
			}
		}
	} else {
		// 非流式响应：直接从 JSON 中提取 usage
		extractUsage(respBody)
	}
}

// filterResponseForLog 创建响应的浅拷贝，过滤掉 images、MultipleContent 中的图片数据和 Audio.Data 以减少存储压力
func (m *RelayMetrics) filterResponseForLog(resp *transformerModel.InternalLLMResponse) *transformerModel.InternalLLMResponse {
	if resp == nil {
		return nil
	}

	filterMsg := func(msg *transformerModel.Message) *transformerModel.Message {
		if msg == nil {
			return nil
		}
		c := *msg
		c.Images = nil
		if len(c.Content.MultipleContent) > 0 {
			parts := make([]transformerModel.MessageContentPart, 0, len(c.Content.MultipleContent))
			for _, p := range c.Content.MultipleContent {
				if p.Type == "image_url" && p.ImageURL != nil {
					parts = append(parts, transformerModel.MessageContentPart{
						Type:     "image_url",
						ImageURL: &transformerModel.ImageURL{URL: "[image data omitted for storage]"},
					})
				} else {
					parts = append(parts, p)
				}
			}
			c.Content = transformerModel.MessageContent{Content: c.Content.Content, MultipleContent: parts}
		}
		if c.Audio != nil && c.Audio.Data != "" {
			a := *c.Audio
			a.Data = "[audio data omitted for storage]"
			c.Audio = &a
		}
		return &c
	}

	filtered := *resp
	filtered.Choices = make([]transformerModel.Choice, len(resp.Choices))
	for i, choice := range resp.Choices {
		filtered.Choices[i] = choice
		filtered.Choices[i].Message = filterMsg(choice.Message)
		filtered.Choices[i].Delta = filterMsg(choice.Delta)
	}
	return &filtered
}
