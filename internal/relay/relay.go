package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/gin-gonic/gin"
	"github.com/tmaxmax/go-sse"
)

// Handler 处理入站请求并转发到上游服务
func Handler(inboundType inbound.InboundType, c *gin.Context) {
	// 解析请求（同时返回原始 body 供 passthrough 模式使用）
	internalRequest, rawBody, inAdapter, err := parseRequest(inboundType, c)
	if err != nil {
		return
	}
	supportedModels := c.GetString("supported_models")
	if supportedModels != "" {
		supportedModelsArray := strings.Split(supportedModels, ",")
		if !slices.Contains(supportedModelsArray, internalRequest.Model) {
			resp.Error(c, http.StatusBadRequest, "model not supported")
			return
		}
	}

	requestModel := internalRequest.Model
	apiKeyID := c.GetInt("api_key_id")

	// 获取通道分组
	group, err := op.GroupGetEnabledMap(requestModel, c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusNotFound, "model not found")
		return
	}

	// 创建迭代器（策略排序 + 粘性优先）
	iter := balancer.NewIterator(group, apiKeyID, requestModel)
	if iter.Len() == 0 {
		resp.Error(c, http.StatusServiceUnavailable, "no available channel")
		return
	}

	// 初始化 Metrics
	metrics := NewRelayMetrics(apiKeyID, requestModel, internalRequest)

	// 请求级上下文
	req := &relayRequest{
		c:               c,
		inAdapter:       inAdapter,
		internalRequest: internalRequest,
		metrics:         metrics,
		apiKeyID:        apiKeyID,
		requestModel:    requestModel,
		iter:            iter,
		rawBody:         rawBody,
		inboundType:     inboundType,
	}

	var lastErr error

	for iter.Next() {
		select {
		case <-c.Request.Context().Done():
			log.Infof("request context canceled, stopping retry")
			metrics.Save(c.Request.Context(), false, context.Canceled, iter.Attempts())
			return
		default:
		}

		item := iter.Item()

		// 获取通道
		channel, err := op.ChannelGet(item.ChannelID, c.Request.Context())
		if err != nil {
			log.Warnf("failed to get channel %d: %v", item.ChannelID, err)
			iter.Skip(item.ChannelID, 0, fmt.Sprintf("channel_%d", item.ChannelID), fmt.Sprintf("channel not found: %v", err))
			lastErr = err
			continue
		}
		if !channel.Enabled {
			iter.Skip(channel.ID, 0, channel.Name, "channel disabled")
			continue
		}

		availableKeys := channel.GetAvailableKeys()
		if len(availableKeys) == 0 {
			iter.Skip(channel.ID, 0, channel.Name, "no available key")
			continue
		}

		// 出站适配器
		outAdapter := outbound.Get(channel.Type)
		if outAdapter == nil {
			iter.Skip(channel.ID, 0, channel.Name, fmt.Sprintf("unsupported channel type: %d", channel.Type))
			continue
		}

		// 类型兼容性检查
		if internalRequest.IsEmbeddingRequest() && !outbound.IsEmbeddingChannelType(channel.Type) {
			iter.Skip(channel.ID, 0, channel.Name, "channel type not compatible with embedding request")
			continue
		}
		if internalRequest.IsChatRequest() && !outbound.IsChatChannelType(channel.Type) {
			iter.Skip(channel.ID, 0, channel.Name, "channel type not compatible with chat request")
			continue
		}

		// 设置实际模型
		internalRequest.Model = item.ModelName

		// 遍历该渠道的所有可用密钥，尝试故障转移
		keyAttempted := false
		for _, usedKey := range availableKeys {
			select {
			case <-c.Request.Context().Done():
				log.Infof("request context canceled, stopping key retry")
				metrics.Save(c.Request.Context(), false, context.Canceled, iter.Attempts())
				return
			default:
			}

			// 熔断检查
			if iter.SkipCircuitBreak(channel.ID, usedKey.ID, channel.Name) {
				continue
			}

			log.Infof("request model %s, mode: %d, forwarding to channel: %s model: %s key: %d (attempt %d/%d, sticky=%t)",
				requestModel, group.Mode, channel.Name, item.ModelName, usedKey.ID,
				iter.Index()+1, iter.Len(), iter.IsSticky())

			// 构造尝试级上下文 -- 只写变化的 4 个字段
			ra := &relayAttempt{
				relayRequest:         req,
				outAdapter:           outAdapter,
				channel:              channel,
				usedKey:              usedKey,
				firstTokenTimeOutSec: group.FirstTokenTimeOut,
			}

			result := ra.attempt()
			keyAttempted = true
			if result.Success {
				metrics.Save(c.Request.Context(), true, nil, iter.Attempts())
				return
			}
			if result.Written {
				metrics.Save(c.Request.Context(), false, result.Err, iter.Attempts())
				return
			}
			lastErr = result.Err
		}

		if !keyAttempted {
			iter.Skip(channel.ID, 0, channel.Name, "all keys circuit-broken or unavailable")
			continue
		}
	}

	// 所有通道都失败
	metrics.Save(c.Request.Context(), false, lastErr, iter.Attempts())
	resp.Error(c, http.StatusBadGateway, "all channels failed")
}

// attempt 统一管理一次通道尝试的完整生命周期
func (ra *relayAttempt) attempt() attemptResult {
	span := ra.iter.StartAttempt(ra.channel.ID, ra.usedKey.ID, ra.channel.Name)

	// 转发请求
	statusCode, fwdErr := ra.forward()

	// 更新 channel key 状态
	ra.usedKey.StatusCode = statusCode
	ra.usedKey.LastUseTimeStamp = time.Now().Unix()

	if fwdErr == nil {
		// ====== 成功 ======
		ra.collectResponse()
		ra.usedKey.TotalCost += ra.metrics.Stats.InputCost + ra.metrics.Stats.OutputCost
		ra.usedKey.FailCount = 0
		op.ChannelKeyUpdate(ra.usedKey)

		span.End(dbmodel.AttemptSuccess, statusCode, "")

		// Channel 维度统计
		op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
			WaitTime:       span.Duration().Milliseconds(),
			RequestSuccess: 1,
		})

		// 熔断器：记录成功
		balancer.RecordSuccess(ra.channel.ID, ra.usedKey.ID, ra.internalRequest.Model)
		// 会话保持：更新粘性记录
		balancer.SetSticky(ra.apiKeyID, ra.requestModel, ra.channel.ID, ra.usedKey.ID)

		ra.metrics.ParamOverride = paramOverrideValue(ra.channel.ParamOverride)

		return attemptResult{Success: true}
	}

	// ====== 失败 ======
	if statusCode == 429 {
		ra.usedKey.FailCount++
		if ra.usedKey.FailCount >= 5 {
			ra.usedKey.Enabled = false
			log.Warnf("key %d auto-disabled after %d consecutive 429 failures", ra.usedKey.ID, ra.usedKey.FailCount)
		}
	} else if statusCode >= 500 || statusCode == 401 || statusCode == 403 {
		ra.usedKey.FailCount++
	} else {
		ra.usedKey.FailCount = 0
	}
	op.ChannelKeyUpdate(ra.usedKey)
	span.End(dbmodel.AttemptFailed, statusCode, fwdErr.Error())

	// Channel 维度统计
	op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
		WaitTime:      span.Duration().Milliseconds(),
		RequestFailed: 1,
	})

	// 熔断器：记录失败
	balancer.RecordFailure(ra.channel.ID, ra.usedKey.ID, ra.internalRequest.Model)

	ra.metrics.ParamOverride = paramOverrideValue(ra.channel.ParamOverride)

	written := ra.c.Writer.Written()
	if written {
		ra.collectResponse()
	}
	return attemptResult{
		Success: false,
		Written: written,
		Err:     fmt.Errorf("channel %s failed: %v", ra.channel.Name, fwdErr),
	}
}

// parseRequest 解析并验证入站请求，返回原始 body 供 passthrough 使用
func parseRequest(inboundType inbound.InboundType, c *gin.Context) (*model.InternalLLMRequest, []byte, model.Inbound, error) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return nil, nil, nil, err
	}

	inAdapter := inbound.Get(inboundType)
	internalRequest, err := inAdapter.TransformRequest(c.Request.Context(), body)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return nil, nil, nil, err
	}

	// Pass through the original query parameters
	internalRequest.Query = c.Request.URL.Query()

	if err := internalRequest.Validate(); err != nil {
		resp.Error(c, http.StatusBadRequest, err.Error())
		return nil, nil, nil, err
	}

	return internalRequest, body, inAdapter, nil
}

// forward 转发请求到上游服务
func (ra *relayAttempt) forward() (int, error) {
	ctx := ra.c.Request.Context()

	// 透传模式：仅在客户端格式与渠道格式一致时生效
	if ra.channel.Passthrough && ra.isPassthroughCompatible() {
		return ra.forwardPassthrough(ctx)
	}

	// 构建出站请求
	outboundRequest, err := ra.outAdapter.TransformRequest(
		ctx,
		ra.internalRequest,
		ra.channel.GetBaseUrl(),
		ra.usedKey.ChannelKey,
	)
	if err != nil {
		log.Warnf("failed to create request: %v", err)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	// 应用 ParamOverride 到请求体
	if ra.channel.ParamOverride != nil && *ra.channel.ParamOverride != "" {
		body, err := io.ReadAll(outboundRequest.Body)
		if err != nil {
			return 0, fmt.Errorf("failed to read body: %w", err)
		}

		var bodyMap map[string]any
		if err := json.Unmarshal(body, &bodyMap); err != nil {
			log.Warnf("failed to unmarshal request body: %v, skipping param_override", err)
			outboundRequest.Body = io.NopCloser(bytes.NewBuffer(body))
			return 0, nil
		}
		var override map[string]any
		if err := json.Unmarshal([]byte(*ra.channel.ParamOverride), &override); err != nil {
			log.Warnf("failed to unmarshal param_override: %v, skipping", err)
			outboundRequest.Body = io.NopCloser(bytes.NewBuffer(body))
			return 0, nil
		}
		maps.Copy(bodyMap, override)
		modifiedBody, err := json.Marshal(bodyMap)
		if err != nil {
			log.Warnf("failed to marshal modified body: %v, skipping param_override", err)
			outboundRequest.Body = io.NopCloser(bytes.NewBuffer(body))
			return 0, nil
		}
		outboundRequest.Body = io.NopCloser(bytes.NewBuffer(modifiedBody))
		outboundRequest.ContentLength = int64(len(modifiedBody))
	}

	// 复制请求头
	ra.copyHeaders(outboundRequest)

	// 发送请求
	response, err := ra.sendRequest(outboundRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer response.Body.Close()

	// 检查响应状态
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return 0, fmt.Errorf("failed to read response body: %w", err)
		}
		return 0, fmt.Errorf("upstream error: %d: %s", response.StatusCode, string(body))
	}

	// 处理响应
	if ra.internalRequest.Stream != nil && *ra.internalRequest.Stream {
		if err := ra.handleStreamResponse(ctx, response); err != nil {
			return 0, err
		}
		return response.StatusCode, nil
	}
	if err := ra.handleResponse(ctx, response); err != nil {
		return 0, err
	}
	return response.StatusCode, nil
}

// isPassthroughCompatible 检查客户端入站格式是否与渠道出站格式一致
// 一致时才可透传，否则仍需协议转换
func (ra *relayAttempt) isPassthroughCompatible() bool {
	switch ra.inboundType {
	case inbound.InboundTypeOpenAIChat:
		return ra.channel.Type == outbound.OutboundTypeOpenAIChat
	case inbound.InboundTypeOpenAIResponse:
		return ra.channel.Type == outbound.OutboundTypeOpenAIResponse
	case inbound.InboundTypeAnthropic:
		return ra.channel.Type == outbound.OutboundTypeAnthropic
	case inbound.InboundTypeOpenAIEmbedding:
		return ra.channel.Type == outbound.OutboundTypeOpenAIEmbedding
	default:
		return false
	}
}

// forwardPassthrough 透传模式：直接转发客户端原始请求，响应同时走 transformer pipeline 提取 usage
func (ra *relayAttempt) forwardPassthrough(ctx context.Context) (int, error) {
	baseUrl := ra.channel.GetBaseUrl()
	if baseUrl == "" {
		return 0, fmt.Errorf("no available base URL")
	}

	// 使用缓存的原始请求体，并将模型名替换为渠道配置中的实际模型名
	rawBody := ra.rawBody
	if rawBody == nil {
		return 0, fmt.Errorf("raw body not available for passthrough")
	}

	// 如果渠道配置了具体模型名，替换请求体中的 model 字段
	if ra.internalRequest.Model != "" {
		var bodyMap map[string]any
		if err := json.Unmarshal(rawBody, &bodyMap); err == nil {
			bodyMap["model"] = ra.internalRequest.Model
			// 透传模式下确保请求中包含 stream_options.include_usage=true
			if streamVal, ok := bodyMap["stream"]; ok {
				if s, ok := streamVal.(bool); ok && s {
					if _, hasSO := bodyMap["stream_options"]; !hasSO {
						bodyMap["stream_options"] = map[string]any{"include_usage": true}
					} else if so, ok := bodyMap["stream_options"].(map[string]any); ok {
						so["include_usage"] = true
					}
				}
			}
			if newBody, err := json.Marshal(bodyMap); err == nil {
				rawBody = newBody
			}
		}
	}

	// 构建出站请求：仅取客户端路径的最后一段 endpoint（去掉 API 前缀）
	clientPath := ra.c.Request.URL.Path
	if idx := strings.LastIndex(clientPath, "/v1/"); idx != -1 {
		clientPath = clientPath[idx+3:]
	}
	fullURL := strings.TrimSuffix(baseUrl, "/") + clientPath
	if ra.c.Request.URL.RawQuery != "" {
		fullURL += "?" + ra.c.Request.URL.RawQuery
	}

	outReq, err := http.NewRequestWithContext(ctx, ra.c.Request.Method, fullURL, bytes.NewReader(rawBody))
	if err != nil {
		return 0, fmt.Errorf("failed to create passthrough request: %w", err)
	}

	// 设置认证头
	outReq.Header.Set("Authorization", "Bearer "+ra.usedKey.ChannelKey)

	// 复制客户端请求头（过滤 hop-by-hop）
	ra.copyHeaders(outReq)

	// 发送请求
	response, err := ra.sendRequest(outReq)
	if err != nil {
		return 0, fmt.Errorf("failed to send passthrough request: %w", err)
	}
	defer response.Body.Close()

	// 检查响应状态
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(response.Body)
		return 0, fmt.Errorf("upstream error: %d: %s", response.StatusCode, string(body))
	}

	// 始终设置 ActualModel
	ra.metrics.SetActualModel(ra.internalRequest.Model)

	isStream := ra.internalRequest.Stream != nil && *ra.internalRequest.Stream

	if isStream {
		return ra.forwardPassthroughStream(ctx, response, rawBody)
	}
	return ra.forwardPassthroughNonStream(ctx, response, rawBody)
}

// forwardPassthroughNonStream 透传非流式：原始响应转发客户端，同时用 transformer 提取 usage
func (ra *relayAttempt) forwardPassthroughNonStream(ctx context.Context, response *http.Response, rawBody []byte) (int, error) {
	// 读取完整响应体
	respBody, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read passthrough response: %w", err)
	}

	// 用 transformer pipeline 提取 usage 和响应内容
	response.Body = io.NopCloser(bytes.NewReader(respBody))
	if internalResp, parseErr := ra.outAdapter.TransformResponse(ctx, response); parseErr == nil && internalResp != nil {
		if internalResp.Usage != nil {
			log.Debugf("passthrough: TransformResponse succeeded, prompt_tokens=%d, completion_tokens=%d",
				internalResp.Usage.PromptTokens, internalResp.Usage.CompletionTokens)
			ra.metrics.SetInternalResponse(internalResp, ra.internalRequest.Model)
		} else {
			log.Debugf("passthrough: TransformResponse succeeded but usage is nil, trying raw extraction")
			ra.metrics.ExtractUsageFromRawResponse(respBody, false)
		}
		// 收集响应内容用于日志（非流式透传）
		ra.collectResponsePassthrough(internalResp, respBody)
	} else {
		log.Debugf("passthrough: TransformResponse failed (%v), trying raw extraction", parseErr)
		ra.metrics.ExtractUsageFromRawResponse(respBody, false)
	}

	// 保存原始请求/响应体用于日志记录
	ra.metrics.SetPassthroughBody(rawBody, respBody)

	// 透传原始响应到客户端
	for key, values := range response.Header {
		for _, value := range values {
			ra.c.Header(key, value)
		}
	}
	ra.c.Status(response.StatusCode)
	if _, err := io.Copy(ra.c.Writer, bytes.NewReader(respBody)); err != nil {
		log.Warnf("failed to copy passthrough response body: %v", err)
	}

	return response.StatusCode, nil
}

// forwardPassthroughStream 透传流式：原始 SSE 流转发客户端，同时用 transformer 逐事件提取 usage
func (ra *relayAttempt) forwardPassthroughStream(ctx context.Context, response *http.Response, rawBody []byte) (int, error) {
	// 设置 SSE 响应头
	ra.c.Header("Content-Type", "text/event-stream")
	ra.c.Header("Cache-Control", "no-cache")
	ra.c.Header("Connection", "keep-alive")
	ra.c.Header("X-Accel-Buffering", "no")

	firstToken := true
	firstTokenTimeOutSec := ra.firstTokenTimeOutSec

	type sseReadResult struct {
		data string
		err  error
	}
	results := make(chan sseReadResult, 64)

	// 在后台 goroutine 中读取 SSE 事件
	go func() {
		defer close(results)
		readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
		for ev, err := range sse.Read(response.Body, readCfg) {
			if err != nil {
				results <- sseReadResult{err: err}
				return
			}
			results <- sseReadResult{data: ev.Data}
		}
	}()

	var firstTokenTimer *time.Timer
	var firstTokenC <-chan time.Time
	if firstTokenTimeOutSec > 0 {
		firstTokenTimer = time.NewTimer(time.Duration(firstTokenTimeOutSec) * time.Second)
		firstTokenC = firstTokenTimer.C
		defer func() {
			if firstTokenTimer != nil {
				firstTokenTimer.Stop()
			}
		}()
	}

	// 收集原始响应体用于日志
	var respBodyBuf bytes.Buffer

	for {
		select {
		case <-ctx.Done():
			log.Infof("client disconnected, stopping passthrough stream")
			return 0, nil
		case <-firstTokenC:
			log.Warnf("first token timeout (%ds) in passthrough stream", firstTokenTimeOutSec)
			return 0, fmt.Errorf("first token timeout (%ds)", firstTokenTimeOutSec)
		case r, ok := <-results:
			if !ok {
				// 流结束，提取 usage 并保存日志
				ra.metrics.ExtractUsageFromRawResponse(respBodyBuf.Bytes(), true)
				ra.metrics.SetPassthroughBody(rawBody, respBodyBuf.Bytes())
				return 200, nil
			}
			if r.err != nil {
				ra.metrics.ExtractUsageFromRawResponse(respBodyBuf.Bytes(), true)
				ra.metrics.SetPassthroughBody(rawBody, respBodyBuf.Bytes())
				return 0, fmt.Errorf("failed to read passthrough stream event: %w", r.err)
			}

			// 写入原始 SSE 事件到客户端
			eventData := "data: " + r.data + "\n\n"
			ra.c.Writer.Write([]byte(eventData))
			ra.c.Writer.Flush()

			// 收集原始响应体用于日志和 usage 提取
			respBodyBuf.WriteString(r.data)
			respBodyBuf.WriteByte('\n')

			// 尝试用 transformer 解析事件以提取 usage
			transformedStream, err := ra.outAdapter.TransformStream(ctx, []byte(r.data))
			if err == nil && transformedStream != nil && transformedStream.Usage != nil {
				ra.metrics.SetInternalResponse(transformedStream, ra.internalRequest.Model)
			}

			if firstToken {
				ra.metrics.SetFirstTokenTime(time.Now())
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
		}
	}
}

// collectResponsePassthrough 从 transformer 解析的响应中收集信息（透传模式）
func (ra *relayAttempt) collectResponsePassthrough(internalResp *model.InternalLLMResponse, respBody []byte) {
	// 透传模式下已经通过 SetInternalResponse 设置了 usage
	// 这里确保响应内容也被记录到日志
	if internalResp != nil {
		ra.metrics.SetInternalResponse(internalResp, ra.internalRequest.Model)
	}
}

// copyHeaders 复制请求头，过滤 hop-by-hop 头
func (ra *relayAttempt) copyHeaders(outboundRequest *http.Request) {
	for key, values := range ra.c.Request.Header {
		if hopByHopHeaders[strings.ToLower(key)] {
			continue
		}
		for _, value := range values {
			outboundRequest.Header.Set(key, value)
		}
	}
	if len(ra.channel.CustomHeader) > 0 {
		for _, header := range ra.channel.CustomHeader {
			outboundRequest.Header.Set(header.HeaderKey, header.HeaderValue)
		}
	}
}

// sendRequest 发送 HTTP 请求
func (ra *relayAttempt) sendRequest(req *http.Request) (*http.Response, error) {
	httpClient, err := helper.ChannelHttpClient(ra.channel)
	if err != nil {
		log.Warnf("failed to get http client: %v", err)
		return nil, err
	}

	response, err := httpClient.Do(req)
	if err != nil {
		log.Warnf("failed to send request: %v", err)
		return nil, err
	}

	return response, nil
}

// handleStreamResponse 处理流式响应
func (ra *relayAttempt) handleStreamResponse(ctx context.Context, response *http.Response) error {
	if ct := response.Header.Get("Content-Type"); ct != "" && !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 16*1024))
		return fmt.Errorf("upstream returned non-SSE content-type %q for stream request: %s", ct, string(body))
	}

	// 设置 SSE 响应头
	ra.c.Header("Content-Type", "text/event-stream")
	ra.c.Header("Cache-Control", "no-cache")
	ra.c.Header("Connection", "keep-alive")
	ra.c.Header("X-Accel-Buffering", "no")

	firstToken := true

	type sseReadResult struct {
		data string
		err  error
	}
	results := make(chan sseReadResult, 1)
	go func() {
		defer close(results)
		readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
		for ev, err := range sse.Read(response.Body, readCfg) {
			if err != nil {
				results <- sseReadResult{err: err}
				return
			}
			results <- sseReadResult{data: ev.Data}
		}
	}()

	var firstTokenTimer *time.Timer
	var firstTokenC <-chan time.Time
	if firstToken && ra.firstTokenTimeOutSec > 0 {
		firstTokenTimer = time.NewTimer(time.Duration(ra.firstTokenTimeOutSec) * time.Second)
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
			log.Infof("client disconnected, stopping stream")
			return nil
		case <-firstTokenC:
			log.Warnf("first token timeout (%ds), switching channel", ra.firstTokenTimeOutSec)
			_ = response.Body.Close()
			return fmt.Errorf("first token timeout (%ds)", ra.firstTokenTimeOutSec)
		case r, ok := <-results:
			if !ok {
				log.Infof("stream end")
				return nil
			}
			if r.err != nil {
				log.Warnf("failed to read event: %v", r.err)
				return fmt.Errorf("failed to read stream event: %w", r.err)
			}

			data, err := ra.transformStreamData(ctx, r.data)
			if err != nil || len(data) == 0 {
				continue
			}
			if firstToken {
				ra.metrics.SetFirstTokenTime(time.Now())
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

			ra.c.Writer.Write(data)
			ra.c.Writer.Flush()
		}
	}
}

// transformStreamData 转换流式数据
func (ra *relayAttempt) transformStreamData(ctx context.Context, data string) ([]byte, error) {
	internalStream, err := ra.outAdapter.TransformStream(ctx, []byte(data))
	if err != nil {
		log.Warnf("failed to transform stream: %v", err)
		return nil, err
	}
	if internalStream == nil {
		return nil, nil
	}

	inStream, err := ra.inAdapter.TransformStream(ctx, internalStream)
	if err != nil {
		log.Warnf("failed to transform stream: %v", err)
		return nil, err
	}

	return inStream, nil
}

// handleResponse 处理非流式响应
func (ra *relayAttempt) handleResponse(ctx context.Context, response *http.Response) error {
	internalResponse, err := ra.outAdapter.TransformResponse(ctx, response)
	if err != nil {
		log.Warnf("failed to transform response: %v", err)
		return fmt.Errorf("failed to transform outbound response: %w", err)
	}

	inResponse, err := ra.inAdapter.TransformResponse(ctx, internalResponse)
	if err != nil {
		log.Warnf("failed to transform response: %v", err)
		return fmt.Errorf("failed to transform inbound response: %w", err)
	}

	ra.c.Data(http.StatusOK, "application/json", inResponse)
	return nil
}

// collectResponse 收集响应信息
func (ra *relayAttempt) collectResponse() {
	internalResponse, err := ra.inAdapter.GetInternalResponse(ra.c.Request.Context())
	if err != nil || internalResponse == nil {
		log.Debugf("collectResponse: no internal response (err=%v, resp=%v)", err, internalResponse != nil)
		return
	}

	log.Debugf("collectResponse: got internal response, usage=%v", internalResponse.Usage != nil)
	ra.metrics.SetInternalResponse(internalResponse, ra.internalRequest.Model)
}

func paramOverrideValue(ptr *string) string {
	if ptr == nil || *ptr == "" {
		return ""
	}
	return *ptr
}
