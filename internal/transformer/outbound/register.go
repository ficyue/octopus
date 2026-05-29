package outbound

// OutboundType is the channel type stored in database as integer.
// It is kept for backward compatibility; new code should use llm.APIFormat.
type OutboundType int

const (
	OutboundTypeOpenAIChat      OutboundType = 0
	OutboundTypeOpenAIResponse  OutboundType = 1
	OutboundTypeAnthropic       OutboundType = 2
	OutboundTypeGemini          OutboundType = 3
	OutboundTypeVolcengine      OutboundType = 4
	OutboundTypeOpenAIEmbedding OutboundType = 5
)
