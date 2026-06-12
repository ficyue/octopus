package handlers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/server/middleware"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/server/router"
	"github.com/bestruirui/octopus/internal/task"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/transformer"
	"github.com/looplj/axonhub/llm/transformer/anthropic"
	"github.com/looplj/axonhub/llm/transformer/openai"
	"github.com/looplj/axonhub/llm/transformer/openai/responses"
	"github.com/gin-gonic/gin"
)

var channelTestTimeout = 30 * time.Second

// ChannelTestRequest 渠道测试请求
type ChannelTestRequest struct {
	ID      int    `json:"id" binding:"required"`
	Message string `json:"message"`
	KeyID   int    `json:"key_id"`
}

// ChannelTestResponse 渠道测试响应
type ChannelTestResponse struct {
	Success   bool   `json:"success"`
	Model     string `json:"model"`
	Content   string `json:"content"`
	LatencyMs int64  `json:"latency_ms"`
	TokensIn  int    `json:"tokens_in"`
	TokensOut int    `json:"tokens_out"`
	Error     string `json:"error,omitempty"`
}

func init() {
	router.NewGroupRouter("/api/v1/channel").
		Use(middleware.Auth()).
		Use(middleware.RequireJSON()).
		AddRoute(
			router.NewRoute("/list", http.MethodGet).
				Handle(listChannel),
		).
		AddRoute(
			router.NewRoute("/create", http.MethodPost).
				Handle(createChannel),
		).
		AddRoute(
			router.NewRoute("/update", http.MethodPost).
				Handle(updateChannel),
		).
		AddRoute(
			router.NewRoute("/enable", http.MethodPost).
				Handle(enableChannel),
		).
		AddRoute(
			router.NewRoute("/delete/:id", http.MethodDelete).
				Handle(deleteChannel),
		).
		AddRoute(
			router.NewRoute("/fetch-model", http.MethodPost).
				Handle(fetchModel),
		).
		AddRoute(
			router.NewRoute("/test", http.MethodPost).
				Handle(testChannel),
		)
	router.NewGroupRouter("/api/v1/channel").
		Use(middleware.Auth()).
		AddRoute(
			router.NewRoute("/sync", http.MethodPost).
				Handle(syncChannel),
		).
		AddRoute(
			router.NewRoute("/last-sync-time", http.MethodGet).
				Handle(getLastSyncTime),
		)
}

func listChannel(c *gin.Context) {
	channels, err := op.ChannelList(c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	for i, channel := range channels {
		stats := op.StatsChannelGet(channel.ID)
		channels[i].Stats = &stats
	}
	resp.Success(c, channels)
}

func createChannel(c *gin.Context) {
	var channel model.Channel
	if err := c.ShouldBindJSON(&channel); err != nil {
		resp.Error(c, http.StatusBadRequest, resp.ErrInvalidJSON)
		return
	}
	if err := op.ChannelCreate(&channel, c.Request.Context()); err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	stats := op.StatsChannelGet(channel.ID)
	channel.Stats = &stats
	go func(channel *model.Channel) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		modelStr := channel.Model + "," + channel.CustomModel
		modelArray := strings.Split(modelStr, ",")
		helper.LLMPriceAddToDB(modelArray, ctx)
		helper.ChannelBaseUrlDelayUpdate(channel, ctx)
		helper.ChannelAutoGroup(channel, ctx)
	}(&channel)
	resp.Success(c, channel)
}

func updateChannel(c *gin.Context) {
	var req model.ChannelUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		resp.Error(c, http.StatusBadRequest, resp.ErrInvalidJSON)
		return
	}
	channel, err := op.ChannelUpdate(&req, c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	stats := op.StatsChannelGet(channel.ID)
	channel.Stats = &stats
	go func(channel *model.Channel) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		modelStr := channel.Model + "," + channel.CustomModel
		modelArray := strings.Split(modelStr, ",")
		helper.LLMPriceAddToDB(modelArray, ctx)
		helper.ChannelBaseUrlDelayUpdate(channel, ctx)
		helper.ChannelAutoGroup(channel, ctx)
	}(channel)
	resp.Success(c, channel)
}

func enableChannel(c *gin.Context) {
	var request struct {
		ID      int  `json:"id"`
		Enabled bool `json:"enabled"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		resp.Error(c, http.StatusBadRequest, resp.ErrInvalidJSON)
		return
	}
	if err := op.ChannelEnabled(request.ID, request.Enabled, c.Request.Context()); err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	resp.Success(c, nil)
}

func deleteChannel(c *gin.Context) {
	id := c.Param("id")
	idNum, err := strconv.Atoi(id)
	if err != nil {
		resp.Error(c, http.StatusBadRequest, resp.ErrInvalidParam)
		return
	}
	if err := op.ChannelDel(idNum, c.Request.Context()); err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	resp.Success(c, nil)
}
func fetchModel(c *gin.Context) {
	var request model.Channel
	if err := c.ShouldBindJSON(&request); err != nil {
		resp.Error(c, http.StatusBadRequest, resp.ErrInvalidJSON)
		return
	}
	models, err := helper.FetchModels(c.Request.Context(), request)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	resp.Success(c, models)
}

func syncChannel(c *gin.Context) {
	task.SyncModelsTask()
	resp.Success(c, nil)
}

func getLastSyncTime(c *gin.Context) {
	time := task.GetLastSyncModelsTime()
	resp.Success(c, time)
}

func testChannel(c *gin.Context) {
	var req ChannelTestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		resp.Error(c, http.StatusBadRequest, resp.ErrInvalidJSON)
		return
	}

	channel, err := op.ChannelGet(req.ID, c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusNotFound, "channel not found")
		return
	}
	if !channel.Enabled {
		resp.Error(c, http.StatusBadRequest, "channel is disabled")
		return
	}

	usedKey := channel.GetChannelKey()
	if req.KeyID > 0 {
		for _, k := range channel.Keys {
			if k.ID == req.KeyID {
				usedKey = k
				break
			}
		}
	}
	if usedKey.ChannelKey == "" {
		resp.Error(c, http.StatusBadRequest, "no available API key")
		return
	}

	baseUrl := channel.GetBaseUrl()
	if baseUrl == "" {
		resp.Error(c, http.StatusBadRequest, "no available base URL")
		return
	}

	outAdapter, err := testNewOutbound(channel.Type, baseUrl, usedKey.ChannelKey)
	if outAdapter == nil || err != nil {
		resp.Error(c, http.StatusBadRequest, "unsupported channel type")
		return
	}

	testMessage := req.Message
	if testMessage == "" {
		testMessage = "Hi"
	}

	var testModel string
	for _, field := range []string{channel.Model, channel.CustomModel} {
		for _, m := range strings.Split(field, ",") {
			m = strings.TrimSpace(m)
			if m != "" {
				testModel = m
				break
			}
		}
		if testModel != "" {
			break
		}
	}
	if testModel == "" {
		testModel = "gpt-3.5-turbo"
	}

	stream := false
	llmReq := &llm.Request{
		Model:       testModel,
		MaxTokens:   int64Ptr(16384),
		Temperature: float64Ptr(0.7),
		Stream:      &stream,
		Messages: []llm.Message{
			{
				Role: "user",
				Content: llm.MessageContent{
					Content: &testMessage,
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), channelTestTimeout)
	defer cancel()

	outReq, err := outAdapter.TransformRequest(ctx, llmReq)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, fmt.Sprintf("failed to build request: %v", err))
		return
	}

	for _, header := range channel.CustomHeader {
		if header.HeaderKey != "" {
			outReq.Headers.Set(header.HeaderKey, header.HeaderValue)
		}
	}

	httpClient, err := helper.ChannelHttpClient(channel)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, fmt.Sprintf("failed to create http client: %v", err))
		return
	}

	startTime := time.Now()
	httpReq, err := httpclient.BuildHttpRequest(ctx, outReq)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, fmt.Sprintf("failed to build http request: %v", err))
		return
	}

	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, fmt.Sprintf("request failed: %v", err))
		return
	}
	defer httpResp.Body.Close()

	latencyMs := time.Since(startTime).Milliseconds()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		resp.Success(c, ChannelTestResponse{
			Success:   false,
			LatencyMs: latencyMs,
			Error:     fmt.Sprintf("failed to read response: %v", err),
		})
		return
	}

	if httpResp.StatusCode != http.StatusOK {
		resp.Success(c, ChannelTestResponse{
			Success:   false,
			LatencyMs: latencyMs,
			Error:     fmt.Sprintf("HTTP %d: %s", httpResp.StatusCode, string(respBody)),
		})
		return
	}

	clientResp := &httpclient.Response{
		StatusCode: httpResp.StatusCode,
		Headers:    httpResp.Header,
		Body:       respBody,
	}

	llmResp, err := outAdapter.TransformResponse(ctx, clientResp)
	if err != nil {
		resp.Success(c, ChannelTestResponse{
			Success:   false,
			LatencyMs: latencyMs,
			Error:     fmt.Sprintf("failed to parse response: %v", err),
		})
		return
	}

	log.Infof("testChannel: model=%s, choices=%d, usage=%+v, resp_preview=%s",
		llmResp.Model, len(llmResp.Choices), llmResp.Usage,
		string(respBody[:min(len(respBody), 500)]))

	result := ChannelTestResponse{
		Success:   true,
		Model:     llmResp.Model,
		LatencyMs: latencyMs,
	}

	if llmResp.Usage != nil {
		result.TokensIn = int(llmResp.Usage.PromptTokens)
		result.TokensOut = int(llmResp.Usage.CompletionTokens)
	}

	if len(llmResp.Choices) > 0 && llmResp.Choices[0].Message != nil {
		msg := llmResp.Choices[0].Message
		if msg.Content.Content != nil && *msg.Content.Content != "" {
			result.Content = *msg.Content.Content
		} else if msg.ReasoningContent != nil && *msg.ReasoningContent != "" {
			result.Content = *msg.ReasoningContent
		}
	}
	// 即使没有文本内容，只要请求成功且有 token 用量，就视为连通成功
	if result.Content == "" && result.TokensOut > 0 {
		result.Content = "(模型响应成功，无文本内容)"
	}

	resp.Success(c, result)
}

func testNewOutbound(channelType outbound.OutboundType, baseURL, key string) (transformer.Outbound, error) {
	switch channelType {
	case outbound.OutboundTypeOpenAIChat:
		return openai.NewOutboundTransformer(baseURL, key)
	case outbound.OutboundTypeOpenAIResponse:
		return responses.NewOutboundTransformer(baseURL, key)
	case outbound.OutboundTypeAnthropic:
		return anthropic.NewOutboundTransformer(baseURL, key)
	case outbound.OutboundTypeGemini:
		return openai.NewOutboundTransformer(baseURL, key)
	case outbound.OutboundTypeOpenAIEmbedding:
		return openai.NewOutboundTransformer(baseURL, key)
	default:
		return nil, fmt.Errorf("unsupported channel type: %d", channelType)
	}
}

func int64Ptr(v int64) *int64   { return &v }
func float64Ptr(v float64) *float64 { return &v }
