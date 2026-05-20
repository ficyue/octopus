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
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/server/middleware"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/server/router"
	"github.com/bestruirui/octopus/internal/task"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

var channelTestTimeout = 30 * time.Second

// ChannelTestRequest 渠道测试请求
type ChannelTestRequest struct {
	ID      int    `json:"id" binding:"required"`
	Message string `json:"message"`
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
	if usedKey.ChannelKey == "" {
		resp.Error(c, http.StatusBadRequest, "no available API key")
		return
	}

	baseUrl := channel.GetBaseUrl()
	if baseUrl == "" {
		resp.Error(c, http.StatusBadRequest, "no available base URL")
		return
	}

	outAdapter := outbound.Get(channel.Type)
	if outAdapter == nil {
		resp.Error(c, http.StatusBadRequest, "unsupported channel type")
		return
	}

	testMessage := req.Message
	if testMessage == "" {
		testMessage = "Hello, please say hi and introduce yourself briefly."
	}

	internalReq := &transformerModel.InternalLLMRequest{
		Model:       "",
		MaxTokens:   ptrInt64(128),
		Temperature: ptrFloat64(0.7),
		Messages: []transformerModel.Message{
			{
				Role: "user",
				Content: transformerModel.MessageContent{
					Content: &testMessage,
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), channelTestTimeout)
	defer cancel()

	httpReq, err := outAdapter.TransformRequest(ctx, internalReq, baseUrl, usedKey.ChannelKey)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, fmt.Sprintf("failed to build request: %v", err))
		return
	}

	for _, header := range channel.CustomHeader {
		if header.HeaderKey != "" {
			httpReq.Header.Set(header.HeaderKey, header.HeaderValue)
		}
	}

	httpClient, err := helper.ChannelHttpClient(channel)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, fmt.Sprintf("failed to create http client: %v", err))
		return
	}

	startTime := time.Now()
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, fmt.Sprintf("request failed: %v", err))
		return
	}
	defer httpResp.Body.Close()

	latencyMs := time.Since(startTime).Milliseconds()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		resp.Success(c, ChannelTestResponse{
			Success:   false,
			LatencyMs: latencyMs,
			Error:     fmt.Sprintf("HTTP %d: %s", httpResp.StatusCode, string(body)),
		})
		return
	}

	internalResp, err := outAdapter.TransformResponse(ctx, httpResp)
	if err != nil {
		resp.Success(c, ChannelTestResponse{
			Success:   false,
			LatencyMs: latencyMs,
			Error:     fmt.Sprintf("failed to parse response: %v", err),
		})
		return
	}

	result := ChannelTestResponse{
		Success:   true,
		Model:     internalResp.Model,
		LatencyMs: latencyMs,
	}

	if internalResp.Usage != nil {
		result.TokensIn = int(internalResp.Usage.PromptTokens)
		result.TokensOut = int(internalResp.Usage.CompletionTokens)
	}

	if len(internalResp.Choices) > 0 && internalResp.Choices[0].Message != nil {
		if internalResp.Choices[0].Message.Content.Content != nil {
			result.Content = *internalResp.Choices[0].Message.Content.Content
		}
	}

	resp.Success(c, result)
}

func ptrInt64(v int64) *int64   { return &v }
func ptrFloat64(v float64) *float64 { return &v }
