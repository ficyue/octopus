package relay

import (
	"fmt"

	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/transformer"
	"github.com/looplj/axonhub/llm/transformer/anthropic"
	"github.com/looplj/axonhub/llm/transformer/openai"
	"github.com/looplj/axonhub/llm/transformer/openai/responses"
)

// outboundTypeToAPIFormat maps our internal OutboundType (stored in DB as int)
// to axonhub/llm's APIFormat (string). This preserves backward compatibility
// with existing database records while using the new transformer library.
func outboundTypeToAPIFormat(channelType outbound.OutboundType) (llm.APIFormat, error) {
	switch channelType {
	case outbound.OutboundTypeOpenAIChat:
		return llm.APIFormatOpenAIChatCompletion, nil
	case outbound.OutboundTypeOpenAIResponse:
		return llm.APIFormatOpenAIResponse, nil
	case outbound.OutboundTypeAnthropic:
		return llm.APIFormatAnthropicMessage, nil
	case outbound.OutboundTypeGemini:
		return llm.APIFormatGeminiContents, nil
	case outbound.OutboundTypeOpenAIEmbedding:
		return llm.APIFormatOpenAIEmbedding, nil
	default:
		return "", fmt.Errorf("unsupported channel type: %d", channelType)
	}
}

// newInbound creates an inbound transformer for the given API format.
func newInbound(format llm.APIFormat) transformer.Inbound {
	switch format {
	case llm.APIFormatOpenAIChatCompletion:
		return openai.NewInboundTransformer()
	case llm.APIFormatOpenAIResponse:
		return responses.NewInboundTransformer()
	case llm.APIFormatAnthropicMessage:
		return anthropic.NewInboundTransformer()
	case llm.APIFormatOpenAIEmbedding:
		return openai.NewEmbeddingInboundTransformer()
	default:
		return nil
	}
}

// newOutbound creates an outbound transformer for the given API format.
func newOutbound(format llm.APIFormat, baseURL, key string) (transformer.Outbound, error) {
	switch format {
	case llm.APIFormatOpenAIChatCompletion, llm.APIFormatOpenAIResponse:
		return openai.NewOutboundTransformer(baseURL, key)
	case llm.APIFormatAnthropicMessage:
		return anthropic.NewOutboundTransformer(baseURL, key)
	case llm.APIFormatGeminiContents:
		return openai.NewOutboundTransformer(baseURL, key) // Gemini via OpenAI compat
	case llm.APIFormatOpenAIEmbedding:
		return openai.NewOutboundTransformer(baseURL, key)
	default:
		return nil, fmt.Errorf("unsupported API format: %s", format)
	}
}
