package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/gin-gonic/gin"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/pipeline/stream"
	"github.com/looplj/axonhub/llm/streams"
	"github.com/looplj/axonhub/llm/transformer"
)

// Handler 返回处理入站请求并转发到上游服务的 Gin handler。
func Handler(inboundType llm.APIFormat) gin.HandlerFunc {
	inAdapter := newInbound(inboundType)
	return func(c *gin.Context) {
		run, err := newRelayRun(c, inboundType, inAdapter)
		if err != nil {
			return
		}
		run.run()
	}
}

func newRelayRun(c *gin.Context, inboundType llm.APIFormat, inAdapter transformer.Inbound) (*relayRun, error) {
	internalRequest, err := parseRequest(c, inboundType, inAdapter)
	if err != nil {
		return nil, err
	}

	// Anthropic 入站请求如果客户端未指定输出 token 上限，给一个更大的默认值，
	// 避免 thinking 模型在 8192 上限内被思考过程占满，导致正文截断或空响应。
	if inboundType == llm.APIFormatAnthropicMessage && internalRequest.MaxTokens == nil && internalRequest.MaxCompletionTokens == nil {
		defaultMaxTokens := int64(64000)
		internalRequest.MaxTokens = &defaultMaxTokens
	}

	if supportedModels := c.GetString("supported_models"); supportedModels != "" {
		if !slices.Contains(strings.Split(supportedModels, ","), internalRequest.Model) {
			err := errors.New("model not supported")
			resp.Error(c, http.StatusBadRequest, err.Error())
			return nil, err
		}
	}

	group, err := op.GroupGetEnabledMap(internalRequest.Model, c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusNotFound, "model not found")
		return nil, err
	}

	apiKeyID := c.GetInt("api_key_id")
	iter := balancer.NewIterator(group, apiKeyID, internalRequest.Model)
	if iter.Len() == 0 {
		err := errors.New("no available channel")
		resp.Error(c, http.StatusServiceUnavailable, err.Error())
		return nil, err
	}

	return &relayRun{
		c:               c,
		inboundType:     inboundType,
		inAdapter:       inAdapter,
		internalRequest: internalRequest,
		metrics: &RelayMetrics{
			APIKeyID:        apiKeyID,
			RequestModel:    internalRequest.Model,
			ActualModel:     internalRequest.Model,
			StartTime:       time.Now(),
			InternalRequest: internalRequest,
		},
		iter:  iter,
		group: group,
	}, nil
}

func (r *relayRun) run() {
	ctx := r.c.Request.Context()
	var lastErr error

	for r.iter.Next() {
		select {
		case <-ctx.Done():
			log.Infof("request context canceled, stopping retry")
			r.metrics.Save(ctx, false, context.Canceled, r.iter.Attempts())
			return
		default:
		}

		// 内层循环：遍历当前渠道的所有可用 key
		triedKeyIDs := make(map[int]bool)
		for {
			attempt, err := r.prepareAttempt(triedKeyIDs)
			if err != nil {
				lastErr = err
				break
			}
			if attempt == nil {
				break
			}

			triedKeyIDs[attempt.usedKey.ID] = true
			written, err := attempt.run()
			if err == nil {
				r.metrics.Save(ctx, true, nil, r.iter.Attempts())
				return
			}
			if written {
				r.metrics.Save(ctx, false, err, r.iter.Attempts())
				return
			}
			lastErr = err
			// 当前 key 失败，继续尝试下一个 key
		}
	}

	if lastErr == nil {
		lastErr = errors.New("all channels failed")
	}
	r.metrics.Save(ctx, false, lastErr, r.iter.Attempts())
	if hideUpstreamError, _ := op.SettingGetBool(dbmodel.SettingKeyHideUpstreamError); hideUpstreamError {
		resp.Error(r.c, http.StatusBadGateway, "all channels failed")
	} else {
		errMsg := "all channels failed"
		if lastErr != nil {
			errMsg = lastErr.Error()
		}
		resp.Error(r.c, http.StatusBadGateway, errMsg)
	}
}

// prepareAttempt 遍历当前渠道的所有可用 key，逐个尝试。
// 如果所有 key 都因熔断跳过，返回 nil 以便上层继续尝试下一个渠道。
func (r *relayRun) prepareAttempt(triedKeyIDs map[int]bool) (*relayAttempt, error) {
	item := r.iter.Item()
	channel, err := op.ChannelGet(item.ChannelID, r.c.Request.Context())
	if err != nil {
		log.Warnf("failed to get channel %d: %v", item.ChannelID, err)
		r.iter.Skip(item.ChannelID, 0, fmt.Sprintf("channel_%d", item.ChannelID), fmt.Sprintf("channel not found: %v", err))
		return nil, err
	}
	if !channel.Enabled {
		r.iter.Skip(channel.ID, 0, channel.Name, "channel disabled")
		return nil, nil
	}

	availableKeys := channel.GetAvailableKeys()
	if len(availableKeys) == 0 {
		r.iter.Skip(channel.ID, 0, channel.Name, "no available key")
		return nil, nil
	}

	apiFormat, _ := outboundTypeToAPIFormat(channel.Type)

	// 遍历该渠道的所有可用 key
	for _, usedKey := range availableKeys {
		if triedKeyIDs[usedKey.ID] {
			continue
		}
		if r.iter.SkipCircuitBreak(channel.ID, usedKey.ID, channel.Name) {
			continue
		}

		outAdapter, err := newOutbound(apiFormat, channel.GetBaseUrl(), usedKey.ChannelKey)
		if err != nil {
			r.iter.Skip(channel.ID, usedKey.ID, channel.Name, err.Error())
			continue
		}

		r.internalRequest.Model = item.ModelName
		r.metrics.ActualModel = item.ModelName
		r.metrics.ParamOverride = ""
		log.Infof("request model %s, mode: %d, forwarding to channel: %s model: %s key: %d (attempt %d/%d, sticky=%t)",
			r.metrics.RequestModel, r.group.Mode, channel.Name, item.ModelName, usedKey.ID,
			r.iter.Index()+1, r.iter.Len(), r.iter.IsSticky())

		return &relayAttempt{
			relayRun:   r,
			outAdapter: outAdapter,
			channel:    channel,
			usedKey:    usedKey,
		}, nil
	}

	r.iter.Skip(channel.ID, 0, channel.Name, "all keys circuit-broken or unavailable")
	return nil, nil
}

// run 统一管理一次通道尝试的完整生命周期。
func (ra *relayAttempt) run() (bool, error) {
	span := ra.iter.StartAttempt(ra.channel.ID, ra.usedKey.ID, ra.channel.Name)

	upstreamStatusCode, fwdErr := ra.forward()
	if fwdErr == nil && upstreamStatusCode == 0 {
		upstreamStatusCode = http.StatusOK
	}
	ra.usedKey.StatusCode = upstreamStatusCode
	ra.usedKey.LastUseTimeStamp = time.Now().Unix()

	if fwdErr == nil {
		ra.usedKey.FailCount = 0
		ra.usedKey.TotalCost += ra.metrics.Stats.InputCost + ra.metrics.Stats.OutputCost
		op.ChannelKeyUpdate(ra.usedKey)

		span.End(dbmodel.AttemptSuccess, upstreamStatusCode, "")
		op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
			WaitTime:       span.Duration().Milliseconds(),
			RequestSuccess: 1,
		})
		balancer.RecordSuccess(ra.channel.ID, ra.usedKey.ID, ra.internalRequest.Model)
		balancer.SetSticky(ra.metrics.APIKeyID, ra.metrics.RequestModel, ra.channel.ID, ra.usedKey.ID)
		return false, nil
	}

	// 429 连续失败自动禁用 key
	if upstreamStatusCode == 429 {
		ra.usedKey.FailCount++
		if ra.usedKey.FailCount >= 5 {
			ra.usedKey.Enabled = false
			log.Warnf("key %d auto-disabled after %d consecutive 429 failures", ra.usedKey.ID, ra.usedKey.FailCount)
		}
	} else if upstreamStatusCode >= 500 || upstreamStatusCode == 401 || upstreamStatusCode == 403 {
		ra.usedKey.FailCount++
	} else {
		ra.usedKey.FailCount = 0
	}
	op.ChannelKeyUpdate(ra.usedKey)
	span.End(dbmodel.AttemptFailed, upstreamStatusCode, fwdErr.Error())
	op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
		WaitTime:      span.Duration().Milliseconds(),
		RequestFailed: 1,
	})
	balancer.RecordFailure(ra.channel.ID, ra.usedKey.ID, ra.internalRequest.Model)

	return ra.c.Writer.Written(), fmt.Errorf("channel %s failed: %v", ra.channel.Name, fwdErr)
}

// parseRequest 解析并验证入站请求
func parseRequest(c *gin.Context, inboundType llm.APIFormat, inAdapter transformer.Inbound) (*llm.Request, error) {
	if inAdapter == nil {
		err := fmt.Errorf("unsupported inbound type: %s", inboundType)
		resp.Error(c, http.StatusBadRequest, err.Error())
		return nil, err
	}

	httpRequest, err := httpclient.ReadHTTPRequest(c.Request)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return nil, err
	}

	internalRequest, err := inAdapter.TransformRequest(c.Request.Context(), httpRequest)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if errors.Is(err, transformer.ErrInvalidRequest) {
			statusCode = http.StatusBadRequest
		}
		resp.Error(c, statusCode, err.Error())
		return nil, err
	}
	if internalRequest.RawRequest == nil {
		internalRequest.RawRequest = httpRequest
	}

	return internalRequest, nil
}

// forward 转发请求到上游服务
func (ra *relayAttempt) forward() (int, error) {
	ctx := ra.c.Request.Context()
	// 为上游请求创建独立 context，客户端断开后仍可 drain 上游流获取 usage
	upstreamCtx, upstreamCancel := context.WithCancel(context.Background())
	defer upstreamCancel()
	if ra.internalRequest.RawRequest == nil {
		return 0, fmt.Errorf("missing raw request")
	}

	httpClient, err := helper.ChannelHttpClient(ra.channel)
	if err != nil {
		log.Warnf("failed to get http client: %v", err)
		return 0, err
	}

	relayMiddleware := &relayPipelineMiddleware{attempt: ra}
	result, err := pipeline.NewFactory(httpclient.NewHttpClientWithClient(httpClient)).
		Pipeline(
			&parsedRequestInbound{Inbound: ra.inAdapter, request: ra.internalRequest},
			ra.outAdapter,
			pipeline.WithMiddlewares(stream.EnsureUsage(), relayMiddleware),
		).
		Process(upstreamCtx, ra.internalRequest.RawRequest)
	if err != nil {
		return relayMiddleware.upstreamStatusCode, err
	}
	if result == nil {
		return 0, fmt.Errorf("empty pipeline result")
	}
	respPreview := ""
	if result.Response != nil && len(result.Response.Body) > 0 {
		respPreview = string(result.Response.Body[:min(len(result.Response.Body), 500)])
	}
	clientStreamVal := "nil"
	if ra.internalRequest.Stream != nil {
		clientStreamVal = fmt.Sprintf("%t", *ra.internalRequest.Stream)
	}
	log.Infof("client stream=%s, pipeline result: stream=%t, response=%v, body_len=%d, preview=%s",
		clientStreamVal,
		result.Stream, result.Response != nil,
		func() int {
			if result.Response != nil {
				return len(result.Response.Body)
			}
			return 0
		}(),
		respPreview)
	if result.Stream {
		// 客户端未请求流式但 pipeline 返回了流（上游自动升级），需要聚合成完整响应。
		if ra.internalRequest.Stream == nil || !*ra.internalRequest.Stream {
			log.Infof("client requested non-stream but got stream, auto-aggregating")
			response, err := ra.autoAggregateStream(ctx, result.EventStream)
			if err != nil {
				return http.StatusOK, err
			}
			response.Body = patchResponseForToolCalls(response.Body, ra.inboundType)
			ra.metrics.InternalResponse = response.Body
			ra.c.Data(http.StatusOK, "application/json", response.Body)
			return http.StatusOK, nil
		}
		if err := ra.writeStream(ctx, result.EventStream); err != nil {
			return http.StatusOK, err
		}
		return http.StatusOK, nil
	}
	if result.Response == nil {
		return 0, fmt.Errorf("empty pipeline response")
	}
	result.Response.Body = patchResponseForToolCalls(result.Response.Body, ra.inboundType)
	ra.metrics.InternalResponse = result.Response.Body
	statusCode := result.Response.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	contentType := "application/json"
	if result.Response.Headers != nil {
		for key, values := range result.Response.Headers {
			for _, value := range values {
				ra.c.Header(key, value)
			}
		}
		if result.Response.Headers.Get("Content-Type") != "" {
			contentType = result.Response.Headers.Get("Content-Type")
		}
	}
	ra.c.Data(statusCode, contentType, result.Response.Body)
	// 兜底：pipeline 可能没有触发 OnOutboundLlmResponse（某些上游格式解析失败），用原始响应兜底。
	if ra.metrics.usage == nil {
		log.Warnf("no usage extracted for non-stream response: body_len=%d, status=%d", len(result.Response.Body), statusCode)
		if ra.usageOverride != nil {
			ra.metrics.RecordUsage(ra.usageOverride)
		}
	}
	return statusCode, nil
}

func (ra *relayAttempt) applyChannelRequestOptions(outboundRequest *httpclient.Request) {
	// ParamOverride 只覆盖 JSON 请求体；multipart 图片编辑等请求不能按 map 合并。
	if ra.channel.ParamOverride != nil && *ra.channel.ParamOverride != "" && strings.Contains(strings.ToLower(outboundRequest.Headers.Get("Content-Type")+" "+outboundRequest.ContentType), "application/json") {
		var bodyMap map[string]any
		if err := json.Unmarshal(outboundRequest.Body, &bodyMap); err != nil {
			log.Warnf("failed to unmarshal request body: %v, skipping param_override", err)
		} else {
			var override map[string]any
			if err := json.Unmarshal([]byte(*ra.channel.ParamOverride), &override); err != nil {
				log.Warnf("failed to unmarshal param_override: %v, skipping", err)
			} else {
				maps.Copy(bodyMap, override)
				modifiedBody, err := json.Marshal(bodyMap)
				if err != nil {
					log.Warnf("failed to marshal modified body: %v, skipping param_override", err)
				} else {
					outboundRequest.Body = modifiedBody
					ra.metrics.ParamOverride = *ra.channel.ParamOverride
				}
			}
		}
	}
	for _, header := range ra.channel.CustomHeader {
		// pipeline 在 raw request middleware 前已经写入 Auth；同名敏感头保持认证配置优先，延续旧 BuildHttpRequest 的覆盖顺序。
		if outboundRequest.Headers.Get(header.HeaderKey) != "" && httpclient.IsSensitiveHeader(header.HeaderKey) {
			continue
		}
		outboundRequest.Headers.Set(header.HeaderKey, header.HeaderValue)
	}
}

// writeStream 把 pipeline 输出的客户端格式流写回请求方，并保留首 token 超时切换通道的行为。
func (ra *relayAttempt) writeStream(ctx context.Context, clientStream streams.Stream[*httpclient.StreamEvent]) error {
	if clientStream == nil {
		return fmt.Errorf("empty pipeline stream")
	}

	// 设置 SSE 响应头
	ra.c.Header("Content-Type", "text/event-stream")
	ra.c.Header("Cache-Control", "no-cache")
	ra.c.Header("Connection", "keep-alive")
	ra.c.Header("X-Accel-Buffering", "no")

	firstToken := true
	sawToolUse := false
	openContentBlocks := make(map[int]bool) // track open content blocks by index
		openToolUseBlocks := make(map[int]bool) // track tool_use blocks that are still open
	responseEvents := make([]*httpclient.StreamEvent, 0, 8)
	type sseReadResult struct {
		event *httpclient.StreamEvent
		err   error
	}
	results := make(chan sseReadResult, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		defer close(results)
		defer clientStream.Close()
		defer func() {
			if r := recover(); r != nil {
				log.Warnf("stream reader panic: %v", r)
				select {
				case results <- sseReadResult{err: fmt.Errorf("stream reader panic: %v", r)}:
				case <-done:
				case <-ctx.Done():
				}
			}
		}()
		// Next 可能阻塞等待上游 token；放到协程里让首 token 超时能及时打断本次通道尝试。
		// 注意：不检查 ctx.Done()（请求 context），因为客户端断开后需要继续 drain 上游流获取 usage。
		// 上游请求用的是 upstreamCtx（独立 context），不受客户端断开影响。
		for clientStream.Next() {
			select {
			case results <- sseReadResult{event: clientStream.Current()}:
			case <-done:
				return
			}
		}
		if err := clientStream.Err(); err != nil {
			select {
			case results <- sseReadResult{err: err}:
			case <-done:
			}
		}
	}()

	firstTokenTimeoutSec := ra.group.FirstTokenTimeOut
	var firstTokenTimer *time.Timer
	var firstTokenC <-chan time.Time
	if firstTokenTimeoutSec > 0 {
		firstTokenTimer = time.NewTimer(time.Duration(firstTokenTimeoutSec) * time.Second)
		firstTokenC = firstTokenTimer.C
		defer func() {
			if firstTokenTimer != nil {
				firstTokenTimer.Stop()
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			log.Infof("client disconnected, draining upstream for usage")
			// 客户端断开后继续从 results channel 读取上游事件，直到拿到 usage 或超时
			drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer drainCancel()
			drainCount := 0
		drainLoop:
			for {
				select {
				case r, ok := <-results:
					if !ok {
						break drainLoop
					}
					if r.err != nil {
						break drainLoop
					}
					if r.event != nil && len(r.event.Data) > 0 {
						drainCount++
						if u := extractUsageFromJSON(r.event.Data); u != nil && (u.PromptTokens > 0 || u.CompletionTokens > 0) {
							ra.metrics.RecordUsage(u)
							break drainLoop
						}
					}
				case <-drainCtx.Done():
					break drainLoop
				}
			}
			_ = clientStream.Close()
			_ = clientStream.Close()
			// 客户端断开时仍尝试从已收集的事件中提取 usage
			if ra.metrics.usage == nil {
				if ra.usageOverride != nil {
					ra.metrics.RecordUsage(ra.usageOverride)
				} else {
					for i := len(responseEvents) - 1; i >= 0; i-- {
						if len(responseEvents[i].Data) > 0 {
							if u := extractUsageFromJSON(responseEvents[i].Data); u != nil {
								ra.metrics.RecordUsage(u)
								break
							}
						}
					}
				}
			}
			if len(responseEvents) > 0 && len(ra.metrics.InternalResponse) == 0 {
				for i := len(responseEvents) - 1; i >= 0; i-- {
					if len(responseEvents[i].Data) > 0 && !bytes.HasPrefix(responseEvents[i].Data, []byte("[DONE]")) {
						ra.metrics.InternalResponse = responseEvents[i].Data
						break
					}
				}
			}
			return nil
		case <-firstTokenC:
			log.Warnf("first token timeout (%ds), switching channel", firstTokenTimeoutSec)
			_ = clientStream.Close()
			return fmt.Errorf("first token timeout (%ds)", firstTokenTimeoutSec)
		case r, ok := <-results:
			if !ok {
				log.Infof("stream end, events collected: %d", len(responseEvents))
				// Debug: log last 3 events for diagnosis
				if len(responseEvents) > 3 && ra.inboundType == llm.APIFormatAnthropicMessage {
					for i := len(responseEvents) - 3; i < len(responseEvents); i++ {
						evt := responseEvents[i]
						if evt != nil && len(evt.Data) > 0 {
							log.Infof("stream end event[%d/%d] type=%s data=%s", i, len(responseEvents), evt.Type, string(evt.Data[:min(len(evt.Data), 300)]))
						}
					}
				}
				// 修复：部分上游（商汤）的 tool_calls 第一个 chunk arguments="" 导致 pipeline
				// 注意：未关闭的内容块已在 writeStream 事件处理循环中补发 content_block_stop
				// （在 message_delta/message_stop 之前），此处不再重复处理。
				// 补发 finish_reason 终止 chunk：部分上游只发一个大 chunk 不带 finish_reason，客户端会一直等
				if ra.inboundType == llm.APIFormatOpenAIChatCompletion && len(responseEvents) > 0 {
					lastEvent := responseEvents[len(responseEvents)-1]
					if lastEvent != nil && len(lastEvent.Data) > 0 {
						var check struct {
							Choices []struct {
								FinishReason *string `json:"finish_reason"`
								Delta        json.RawMessage `json:"delta"`
							} `json:"choices"`
						}
						needFinish := false
						if json.Unmarshal(lastEvent.Data, &check) == nil && len(check.Choices) > 0 {
							fr := check.Choices[0].FinishReason
							frStr := "<nil>"; if fr != nil { frStr = *fr }; log.Infof("stream end check: finish_reason=%s (nil=%v), delta_has_content=%v", frStr, fr == nil, len(check.Choices[0].Delta) > 0)
							if fr == nil {
								needFinish = true
							}
						} else {
							needFinish = true
						}
						if needFinish {
							log.Infof("stream: last event missing finish_reason, sending stop chunk")
							ra.c.Writer.Write([]byte("data: " + fmt.Sprintf(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"object":"chat.completion.chunk"}`) + "\n\n"))
						}
					}
					// OpenAI 兼容协议要求流末尾发送 [DONE] 标记
					ra.c.Writer.Write([]byte("data: [DONE]\n\n"))
				}

				ra.c.Writer.Flush()
				if len(responseEvents) == 0 {
					// 流式无事件时用兜底 usage
					if ra.usageOverride != nil {
						ra.metrics.RecordUsage(ra.usageOverride)
					}
					return nil
				}
				// 客户端请求流式时，pipeline 只负责边转边写，不会自动生成完整响应体。
				// 这里复用同一个 inbound 聚合器把已经写给客户端的事件合成最终 body，日志只落一次最终响应。
				responseBody, meta, err := ra.inAdapter.AggregateStreamChunks(context.WithoutCancel(ctx), responseEvents)
				if err != nil {
					log.Warnf("failed to aggregate stream response for log: %v", err)
					// 聚合失败时，用最后一个非空事件的 data 作为响应内容兜底
					for i := len(responseEvents) - 1; i >= 0; i-- {
						if len(responseEvents[i].Data) > 0 && !bytes.HasPrefix(responseEvents[i].Data, []byte("[DONE]")) {
							ra.metrics.InternalResponse = responseEvents[i].Data
							// 聚合失败时也尝试从事件数据提取 usage
							if u := extractUsageFromJSON(responseEvents[i].Data); u != nil {
								ra.metrics.RecordUsage(u)
							}
							break
						}
					}
					if ra.metrics.usage == nil && ra.usageOverride != nil {
						ra.metrics.RecordUsage(ra.usageOverride)
					}
					return nil
				}
				ra.metrics.InternalResponse = responseBody
				// 流式场景下，如果标准解析未拿到 usage，用原始流事件中提取的兜底。
				usage := meta.Usage
				if usage == nil {
					usage = ra.usageOverride
				}
				// 最后兜底：从聚合后的响应体中直接提取 usage
				if usage == nil && len(responseBody) > 0 {
					usage = extractUsageFromJSON(responseBody)
				}
				if ra.cachedTokensOverride > 0 && usage != nil &&
					(usage.PromptTokensDetails == nil || usage.PromptTokensDetails.CachedTokens == 0) {
					if usage.PromptTokensDetails == nil {
						usage.PromptTokensDetails = &llm.PromptTokensDetails{}
					}
					usage.PromptTokensDetails.CachedTokens = ra.cachedTokensOverride
				}
				ra.metrics.RecordUsage(usage)
				if usage == nil {
					log.Warnf("no usage extracted for stream response: events=%d, response_len=%d", len(responseEvents), len(responseBody))
				}
				return nil
			} // end if !ok
			if r.err != nil {
				log.Warnf("failed to read event: %v", r.err)
				return fmt.Errorf("failed to read stream event: %w", r.err)
			}

			if r.event == nil || len(r.event.Data) == 0 {
				continue
			}
			// 跳过上游发来的 [DONE] 标记，避免重复写入和误算事件数
			if bytes.Equal(r.event.Data, []byte("[DONE]")) {
				continue
			}
			r.event.Data, sawToolUse = patchStreamEventForToolCalls(r.event.Data, ra.inboundType, sawToolUse)
			// 过滤异常的 content_block_stop：如果没有对应的 content_block_start 就跳过
			// 部分上游（商汤等）会发送多余的 content_block_stop 导致客户端断开
			eventType := extractEventType(r.event.Data)
			if eventType == "content_block_start" {
				var idx struct {
					Index        int    `json:"index"`
					ContentBlock struct {
						Type string `json:"type"`
					} `json:"content_block"`
				}
				if json.Unmarshal(r.event.Data, &idx) == nil {
					openContentBlocks[idx.Index] = true
					if idx.ContentBlock.Type == "tool_use" {
						openToolUseBlocks[idx.Index] = true
					}
				}
			} else if eventType == "content_block_stop" {
				var idx struct {
					Index int `json:"index"`
				}
				if json.Unmarshal(r.event.Data, &idx) == nil {
					if !openContentBlocks[idx.Index] {
						log.Infof("filtering orphan content_block_stop: index=%d (no matching content_block_start)", idx.Index)
						continue
					}
					delete(openContentBlocks, idx.Index)
					delete(openToolUseBlocks, idx.Index)
				}
			} else if eventType == "content_block_delta" {
				// 修复：部分上游（商汤）的 pipeline 转换可能发送 delta 事件但未发送对应的 content_block_start
				// 客户端收到 delta 引用未打开的 index 会报 "Content block not found"
				var idx struct {
					Index int `json:"index"`
				}
				if json.Unmarshal(r.event.Data, &idx) == nil && !openContentBlocks[idx.Index] {
					log.Infof("auto-opening content block: index=%d (received delta without content_block_start)", idx.Index)
					startData, _ := json.Marshal(map[string]any{
						"type":         "content_block_start",
						"index":        idx.Index,
						"content_block": map[string]any{"type": "text", "text": ""},
					})
					fixStart := &httpclient.StreamEvent{Type: "content_block_start", Data: startData}
					responseEvents = append(responseEvents, fixStart)
					if ra.c.Writer != nil {
						ra.c.Writer.Write([]byte("event: content_block_start\n"))
						ra.c.Writer.Write([]byte("data: "))
						ra.c.Writer.Write(startData)
						ra.c.Writer.Write([]byte("\n\n"))
						ra.c.Writer.Flush()
					}
					openContentBlocks[idx.Index] = true
				}
			}
			// 修复：部分上游（商汤）的 pipeline 转换不输出 content_block_stop，导致客户端无法结束内容块。
			// 在 message_delta/message_stop 之前，为所有未关闭的 content block 补发 content_block_stop。
			if (eventType == "message_delta" || eventType == "message_stop") && len(openContentBlocks) > 0 && ra.inboundType == llm.APIFormatAnthropicMessage {
				// 按升序关闭，保证事件顺序
				indices := make([]int, 0, len(openContentBlocks))
				for idx := range openContentBlocks {
					indices = append(indices, idx)
				}
				sort.Ints(indices)
				for _, idx := range indices {
					log.Infof("fixing unclosed content block: index=%d, sending content_block_stop before %s", idx, eventType)
					stopData, _ := json.Marshal(map[string]any{
						"type":  "content_block_stop",
						"index": idx,
					})
					fixEvent := &httpclient.StreamEvent{Type: "content_block_stop", Data: stopData}
					responseEvents = append(responseEvents, fixEvent)
					if ra.c.Writer != nil {
						ra.c.Writer.Write([]byte("event: content_block_stop\n"))
						ra.c.Writer.Write([]byte("data: "))
						ra.c.Writer.Write(stopData)
						ra.c.Writer.Write([]byte("\n\n"))
					}
				}
				ra.c.Writer.Flush()
				for _, idx := range indices {
					delete(openContentBlocks, idx)
					delete(openToolUseBlocks, idx)
				}
			}
			// 这里只临时保存 pipeline 已经转换好的客户端格式事件，正常结束后聚合成最终响应体用于日志；不会把分片逐条落库。
			if len(responseEvents) < 3 {
				log.Infof("stream event[%d] type=%s data_preview=%s", len(responseEvents), r.event.Type, string(r.event.Data[:min(len(r.event.Data), 300)]))
			}
			responseEvents = append(responseEvents, r.event)
			if firstToken {
				ra.metrics.FirstTokenTime = time.Now()
				firstToken = false
				if firstTokenTimer != nil {
					if !firstTokenTimer.Stop() {
						select {
						case <-firstTokenTimer.C:
						default:
						}
					}
					firstTokenTimer = nil
					firstTokenC = nil
				}
			}

			// 直接写入 SSE 格式；有 event type 时写 event 行（Responses/Anthropic），否则只写 data（Chat Completions）
			if r.event.Type != "" {
				ra.c.Writer.Write([]byte("event: " + r.event.Type + "\n"))
			}
			ra.c.Writer.Write([]byte("data: "))
			ra.c.Writer.Write(r.event.Data)
			ra.c.Writer.Write([]byte("\n\n"))
			ra.c.Writer.Flush()
			if len(responseEvents) <= 3 {
				log.Infof("SSEvent written: data_len=%d", len(r.event.Data))
			}
		}
	}
}

// relayPipelineMiddleware 承接 octopus 自己的通道级副作用：
// 1. 在 pipeline 发出上游请求前应用渠道参数覆盖和自定义 header；
// 2. 在上游失败时保存 HTTP 状态码，供 key 冷却、熔断和后续选路使用；
// 3. 在非流式响应转成 llm.Response 后记录 usage。
// axonhub/llm 只提供了部分函数式 middleware 构造器，错误状态码和 llm 响应 usage 这两个回调没有公开构造器，
// 所以这里保留一个很薄的结构体实现完整接口，而不是在 relay 主流程里重复 pipeline 的执行逻辑。
type relayPipelineMiddleware struct {
	pipeline.DummyMiddleware
	attempt            *relayAttempt
	upstreamStatusCode int
}

func (m *relayPipelineMiddleware) Name() string {
	return "octopus_relay"
}

func (m *relayPipelineMiddleware) OnOutboundRawRequest(ctx context.Context, request *httpclient.Request) (*httpclient.Request, error) {
	if request.Headers == nil {
		request.Headers = make(http.Header)
	}
	m.attempt.applyChannelRequestOptions(request)
	return request, nil
}

func (m *relayPipelineMiddleware) OnOutboundRawError(ctx context.Context, err error) {
	var upstreamErr *httpclient.Error
	if errors.As(err, &upstreamErr) {
		// pipeline 会把上游错误转换成统一错误返回；这里在转换前记录原始 HTTP 状态码，用于渠道 key 的后续调度决策。
		m.upstreamStatusCode = upstreamErr.StatusCode
	}
}

func (m *relayPipelineMiddleware) OnOutboundRawStream(ctx context.Context, stream streams.Stream[*httpclient.StreamEvent]) (streams.Stream[*httpclient.StreamEvent], error) {
	if stream == nil {
		log.Warnf("OnOutboundRawStream: stream is nil")
		return stream, nil
	}
	log.Infof("OnOutboundRawStream: stream received")
	// 保存原始上游流引用，客户端断开时用于 drain 读取剩余事件
	m.attempt.rawUpstreamStream = stream
	// 包装原始流，在每个事件中查找 usage 信息
	return streams.Map(stream, func(event *httpclient.StreamEvent) *httpclient.StreamEvent {
		if event == nil || len(event.Data) == 0 {
			return event
		}

		var raw struct {
			Usage *struct {
				PromptTokens        int64 `json:"prompt_tokens"`
				CompletionTokens    int64 `json:"completion_tokens"`
				TotalTokens         int64 `json:"total_tokens"`
				InputTokens         int64 `json:"input_tokens"`
				OutputTokens        int64 `json:"output_tokens"`
				PromptTokensDetails *struct {
					CachedTokens int64 `json:"cached_tokens"`
				} `json:"prompt_tokens_details"`
				InputTokensDetails *struct {
					CachedTokens int64 `json:"cached_tokens"`
				} `json:"input_tokens_details"`
				CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
				CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
			} `json:"usage"`
			Message *struct {
				Usage *struct {
					PromptTokens        int64 `json:"prompt_tokens"`
					CompletionTokens    int64 `json:"completion_tokens"`
					TotalTokens         int64 `json:"total_tokens"`
					InputTokens         int64 `json:"input_tokens"`
					OutputTokens        int64 `json:"output_tokens"`
					PromptTokensDetails *struct {
						CachedTokens int64 `json:"cached_tokens"`
					} `json:"prompt_tokens_details"`
					InputTokensDetails *struct {
						CachedTokens int64 `json:"cached_tokens"`
					} `json:"input_tokens_details"`
					CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
					CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
				} `json:"usage"`
			} `json:"message"`
			Response *struct {
				Usage *struct {
					PromptTokens        int64 `json:"prompt_tokens"`
					CompletionTokens    int64 `json:"completion_tokens"`
					TotalTokens         int64 `json:"total_tokens"`
					InputTokens         int64 `json:"input_tokens"`
					OutputTokens        int64 `json:"output_tokens"`
					PromptTokensDetails *struct {
						CachedTokens int64 `json:"cached_tokens"`
					} `json:"prompt_tokens_details"`
					InputTokensDetails *struct {
						CachedTokens int64 `json:"cached_tokens"`
					} `json:"input_tokens_details"`
					CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
					CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
				} `json:"usage"`
			} `json:"response"`
		}
		if json.Unmarshal(event.Data, &raw) != nil {
			return event
		}
		// 优先使用顶层 usage，其次 message.usage（Anthropic message_start），最后 response.usage（Responses API）
		u := raw.Usage
		if u == nil && raw.Message != nil && raw.Message.Usage != nil {
			u = raw.Message.Usage
		}
		if u == nil && raw.Response != nil && raw.Response.Usage != nil {
			u = raw.Response.Usage
		}
		if u != nil {
			// 提取缓存 token
			cached := int64(0)
			if u.PromptTokensDetails != nil {
				cached = u.PromptTokensDetails.CachedTokens
			}
			if cached == 0 && u.InputTokensDetails != nil {
				cached = u.InputTokensDetails.CachedTokens
			}
			if cached == 0 {
				cached = u.CacheReadInputTokens
			}
			if cached > 0 {
				m.attempt.cachedTokensOverride = cached
			}
			// 保存完整 usage 作为兜底
			inputTokens := u.PromptTokens
			if inputTokens == 0 {
				inputTokens = u.InputTokens
				// Anthropic 格式: input_tokens 不含缓存，需要加上 cache_read + cache_creation
				inputTokens += u.CacheReadInputTokens + u.CacheCreationInputTokens
			}
			outputTokens := u.CompletionTokens
			if outputTokens == 0 {
				outputTokens = u.OutputTokens
			}
			if inputTokens > 0 || outputTokens > 0 {
				totalTokens := u.TotalTokens
				if totalTokens == 0 {
					totalTokens = inputTokens + outputTokens
				}
				m.attempt.usageOverride = &llm.Usage{
					PromptTokens:     inputTokens,
					CompletionTokens: outputTokens,
					TotalTokens:      totalTokens,
				}
				if cached > 0 {
					m.attempt.usageOverride.PromptTokensDetails = &llm.PromptTokensDetails{CachedTokens: cached}
				}
			}
		} // end if u != nil
		return event
	}), nil
}

func (m *relayPipelineMiddleware) OnOutboundRawResponse(ctx context.Context, response *httpclient.Response) (*httpclient.Response, error) {
	if response != nil && len(response.Body) > 0 {
		log.Infof("OnOutboundRawResponse: status=%d, body_len=%d, preview=%s",
			response.StatusCode, len(response.Body),
			string(response.Body[:min(len(response.Body), 500)]))
		// 从原始 JSON 中提取 usage 信息作为兜底。
		// 部分上游返回 input_tokens_details 而非标准的 prompt_tokens_details，
		// 或者 llm 库解析失败时，这里直接从原始 body 中提取。
		var raw struct {
			Usage *struct {
				PromptTokens        int64 `json:"prompt_tokens"`
				CompletionTokens    int64 `json:"completion_tokens"`
				TotalTokens         int64 `json:"total_tokens"`
				InputTokens         int64 `json:"input_tokens"`
				OutputTokens        int64 `json:"output_tokens"`
				PromptTokensDetails *struct {
					CachedTokens int64 `json:"cached_tokens"`
				} `json:"prompt_tokens_details"`
				InputTokensDetails *struct {
					CachedTokens int64 `json:"cached_tokens"`
				} `json:"input_tokens_details"`
				CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
				CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(response.Body, &raw) == nil && raw.Usage != nil {
			u := raw.Usage
			inputTokens := u.PromptTokens
			if inputTokens == 0 {
				inputTokens = u.InputTokens
			// Anthropic 格式: input_tokens 不含缓存，需要加上 cache_read + cache_creation
			inputTokens += u.CacheReadInputTokens + u.CacheCreationInputTokens
			}
			outputTokens := u.CompletionTokens
			if outputTokens == 0 {
				outputTokens = u.OutputTokens
			}
			totalTokens := u.TotalTokens
			if totalTokens == 0 {
				totalTokens = inputTokens + outputTokens
			}
			// 提取缓存 token
			cached := int64(0)
			if u.PromptTokensDetails != nil {
				cached = u.PromptTokensDetails.CachedTokens
			}
			if cached == 0 && u.InputTokensDetails != nil {
				cached = u.InputTokensDetails.CachedTokens
			}
			if cached == 0 {
				cached = u.CacheReadInputTokens
			}
			if cached > 0 {
				m.attempt.cachedTokensOverride = cached
			}
			// 保存完整 usage 作为兜底
			if inputTokens > 0 || outputTokens > 0 {
				m.attempt.usageOverride = &llm.Usage{
					PromptTokens:     inputTokens,
					CompletionTokens: outputTokens,
					TotalTokens:      totalTokens,
				}
				if cached > 0 {
					m.attempt.usageOverride.PromptTokensDetails = &llm.PromptTokensDetails{CachedTokens: cached}
				}
			}
		}
	}
	return response, nil
}

func (m *relayPipelineMiddleware) OnOutboundLlmResponse(ctx context.Context, response *llm.Response) (*llm.Response, error) {
	if response != nil {
		usage := response.Usage
		// 如果 llm 库解析不到 usage，用原始响应中提取的兜底。
		if usage == nil && m.attempt.usageOverride != nil {
			usage = m.attempt.usageOverride
		}
		// 补充缓存 token 信息
		if m.attempt.cachedTokensOverride > 0 && usage != nil &&
			(usage.PromptTokensDetails == nil || usage.PromptTokensDetails.CachedTokens == 0) {
			if usage.PromptTokensDetails == nil {
				usage.PromptTokensDetails = &llm.PromptTokensDetails{}
			}
			usage.PromptTokensDetails.CachedTokens = m.attempt.cachedTokensOverride
		}
		m.attempt.metrics.RecordUsage(usage)
	}
	return response, nil
}

// autoAggregateStream 把 pipeline 输出的客户端格式流聚合成完整响应体，
// 用于客户端未请求流式但上游返回流式的场景。
func (ra *relayAttempt) autoAggregateStream(ctx context.Context, clientStream streams.Stream[*httpclient.StreamEvent]) (*httpclient.Response, error) {
	if clientStream == nil {
		return nil, fmt.Errorf("stream is nil")
	}
	defer clientStream.Close()

	chunks := make([]*httpclient.StreamEvent, 0, 8)
	for clientStream.Next() {
		event := clientStream.Current()
		if event != nil {
			chunks = append(chunks, event)
		}
	}
	if err := clientStream.Err(); err != nil {
		return nil, fmt.Errorf("stream read error: %w", err)
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no stream chunks")
	}
	log.Infof("autoAggregateStream: chunks=%d, first_chunk_preview=%s",
		len(chunks), func() string {
			if len(chunks) > 0 && len(chunks[0].Data) > 0 {
				return string(chunks[0].Data[:min(len(chunks[0].Data), 300)])
			}
			return ""
		}())

	body, meta, err := ra.inAdapter.AggregateStreamChunks(context.WithoutCancel(ctx), chunks)
	if err != nil {
		return nil, fmt.Errorf("aggregate error: %w", err)
	}

	usage := meta.Usage
	if usage == nil && ra.usageOverride != nil {
		usage = ra.usageOverride
	}
	if usage != nil {
		ra.metrics.RecordUsage(usage)
	}

	return &httpclient.Response{
		StatusCode: http.StatusOK,
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: body,
	}, nil
}

func patchResponseForToolCalls(body []byte, inboundType llm.APIFormat) []byte {
	switch inboundType {
	case llm.APIFormatAnthropicMessage:
		var m map[string]any
		if json.Unmarshal(body, &m) != nil {
			return body
		}
		if sr, ok := m["stop_reason"].(string); ok && sr == "end_turn" {
			if containsAnthropicToolUse(m) {
				m["stop_reason"] = "tool_use"
				if out, err := json.Marshal(m); err == nil {
					return out
				}
			}
		}
		return body
	default:
		var m map[string]any
		if json.Unmarshal(body, &m) != nil {
			return body
		}
		if choices, ok := m["choices"].([]any); ok && len(choices) > 0 {
			if ch, ok := choices[0].(map[string]any); ok {
				if openAIHasToolCalls(ch) {
					if fr, ok := ch["finish_reason"].(string); ok && fr == "stop" {
						ch["finish_reason"] = "tool_calls"
						if out, err := json.Marshal(m); err == nil {
							return out
						}
					}
				}
			}
		}
		return body
	}
}

func containsAnthropicToolUse(resp map[string]any) bool {
	if content, ok := resp["content"].([]any); ok {
		for _, block := range content {
			if b, ok := block.(map[string]any); ok {
				if t, ok := b["type"].(string); ok {
					if strings.HasSuffix(t, "_tool_use") || t == "tool_use" {
						return true
					}
				}
			}
		}
	}
	return false
}

func openAIHasToolCalls(choice map[string]any) bool {
	if msg, ok := choice["message"].(map[string]any); ok {
		if tc, ok := msg["tool_calls"].([]any); ok && len(tc) > 0 {
			return true
		}
	}
	if delta, ok := choice["delta"].(map[string]any); ok {
		if tc, ok := delta["tool_calls"].([]any); ok && len(tc) > 0 {
			return true
		}
	}
	return false
}

func patchStreamEventForToolCalls(data []byte, inboundType llm.APIFormat, sawToolUse bool) ([]byte, bool) {
	switch inboundType {
	case llm.APIFormatAnthropicMessage:
		var m map[string]any
		if json.Unmarshal(data, &m) != nil {
			return data, sawToolUse
		}
		if typ, ok := m["type"].(string); ok && typ == "content_block_start" {
			if cb, ok := m["content_block"].(map[string]any); ok {
				if t, ok := cb["type"].(string); ok {
					if strings.HasSuffix(t, "_tool_use") || t == "tool_use" {
						sawToolUse = true
					}
				}
			}
		}
		if delta, ok := m["delta"].(map[string]any); ok {
			if sr, ok := delta["stop_reason"].(string); ok && sr == "end_turn" && sawToolUse {
				delta["stop_reason"] = "tool_use"
				if out, err := json.Marshal(m); err == nil {
					return out, sawToolUse
				}
			}
		}
		return data, sawToolUse
	default:
		var m map[string]any
		if json.Unmarshal(data, &m) != nil {
			return data, sawToolUse
		}
		if choices, ok := m["choices"].([]any); ok && len(choices) > 0 {
			if ch, ok := choices[0].(map[string]any); ok {
				if openAIHasToolCalls(ch) {
					sawToolUse = true
				}
				if fr, ok := ch["finish_reason"].(string); ok && fr == "stop" && sawToolUse {
					ch["finish_reason"] = "tool_calls"
					if out, err := json.Marshal(m); err == nil {
						return out, sawToolUse
					}
				}
			}
		}
		return data, sawToolUse
	}
}

// extractEventType extracts the "type" field from a JSON event payload.
func extractEventType(data []byte) string {
	var t struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &t) == nil {
		return t.Type
	}
	return ""
}

// extractUsageFromJSON tries to extract usage info from any JSON body.
// Supports:
//   - OpenAI Chat: top-level usage.prompt_tokens/completion_tokens
//   - Anthropic: top-level usage.input_tokens/output_tokens (message_delta events)
//   - Anthropic nested: message.usage.input_tokens/output_tokens (message_start events)
//   - Responses API: response.usage.input_tokens/output_tokens
func extractUsageFromJSON(data []byte) *llm.Usage {
	var raw struct {
		Usage *struct {
			PromptTokens        int64 `json:"prompt_tokens"`
			CompletionTokens    int64 `json:"completion_tokens"`
			TotalTokens         int64 `json:"total_tokens"`
			InputTokens         int64 `json:"input_tokens"`
			OutputTokens        int64 `json:"output_tokens"`
			PromptTokensDetails *struct {
				CachedTokens      int64 `json:"cached_tokens"`
				WriteCachedTokens int64 `json:"write_cached_tokens"`
			} `json:"prompt_tokens_details"`
			InputTokensDetails *struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"input_tokens_details"`
			CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
			CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
		} `json:"usage"`
		Message *struct {
			Usage *struct {
				PromptTokens        int64 `json:"prompt_tokens"`
				CompletionTokens    int64 `json:"completion_tokens"`
				TotalTokens         int64 `json:"total_tokens"`
				InputTokens         int64 `json:"input_tokens"`
				OutputTokens        int64 `json:"output_tokens"`
				PromptTokensDetails *struct {
					CachedTokens      int64 `json:"cached_tokens"`
					WriteCachedTokens int64 `json:"write_cached_tokens"`
				} `json:"prompt_tokens_details"`
				InputTokensDetails *struct {
					CachedTokens int64 `json:"cached_tokens"`
				} `json:"input_tokens_details"`
				CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
				CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
			} `json:"usage"`
		} `json:"message"`
		Response *struct {
			Usage *struct {
				PromptTokens        int64 `json:"prompt_tokens"`
				CompletionTokens    int64 `json:"completion_tokens"`
				TotalTokens         int64 `json:"total_tokens"`
				InputTokens         int64 `json:"input_tokens"`
				OutputTokens        int64 `json:"output_tokens"`
				PromptTokensDetails *struct {
					CachedTokens      int64 `json:"cached_tokens"`
					WriteCachedTokens int64 `json:"write_cached_tokens"`
				} `json:"prompt_tokens_details"`
				InputTokensDetails *struct {
					CachedTokens int64 `json:"cached_tokens"`
				} `json:"input_tokens_details"`
				CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
				CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
			} `json:"usage"`
		} `json:"response"`
	}
	if json.Unmarshal(data, &raw) != nil {
		return nil
	}
	// 优先使用顶层 usage，其次 message.usage（Anthropic message_start），最后 response.usage（Responses API）
	u := raw.Usage
	if u == nil && raw.Message != nil && raw.Message.Usage != nil {
		u = raw.Message.Usage
	}
	if u == nil && raw.Response != nil && raw.Response.Usage != nil {
		u = raw.Response.Usage
	}
	if u == nil {
		return nil
	}
	inputTokens := u.PromptTokens
	if inputTokens == 0 {
		inputTokens = u.InputTokens
		// Anthropic 格式: input_tokens 不含缓存，需要加上 cache_read + cache_creation
		inputTokens += u.CacheReadInputTokens + u.CacheCreationInputTokens
	}
	outputTokens := u.CompletionTokens
	if outputTokens == 0 {
		outputTokens = u.OutputTokens
	}
	if inputTokens == 0 && outputTokens == 0 {
		return nil
	}
	totalTokens := u.TotalTokens
	if totalTokens == 0 {
		totalTokens = inputTokens + outputTokens
	}
	cached := int64(0)
	writeCached := int64(0)
	if u.PromptTokensDetails != nil {
		cached = u.PromptTokensDetails.CachedTokens
		writeCached = u.PromptTokensDetails.WriteCachedTokens
	}
	if cached == 0 && u.InputTokensDetails != nil {
		cached = u.InputTokensDetails.CachedTokens
	}
	if cached == 0 {
		cached = u.CacheReadInputTokens
	}
	usage := &llm.Usage{
		PromptTokens:     inputTokens,
		CompletionTokens: outputTokens,
		TotalTokens:      totalTokens,
	}
	if cached > 0 || writeCached > 0 {
		usage.PromptTokensDetails = &llm.PromptTokensDetails{
			CachedTokens:      cached,
			WriteCachedTokens: writeCached,
		}
	}
	return usage
}

// parsedRequestInbound 让 pipeline 复用 relay 在选路前已经解析好的 llm.Request。
// 这样每次候选通道尝试只重新执行 outbound transform 和 HTTP 请求，不会重复读取或解析客户端 body。
type parsedRequestInbound struct {
	transformer.Inbound
	request *llm.Request
}

func (in *parsedRequestInbound) TransformRequest(ctx context.Context, request *httpclient.Request) (*llm.Request, error) {
	if in.request == nil {
		return nil, fmt.Errorf("missing parsed request")
	}
	// relay 已经为选路解析过请求；pipeline 入口复用该结果，避免每次通道尝试再次解析同一份 body。
	in.request.RawRequest = request
	return in.request, nil
}
