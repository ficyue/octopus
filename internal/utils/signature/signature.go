package signature

import (
	"encoding/base64"
	"strings"
)

// Provider identifies which AI provider generated a signature/reasoning blob.
type Provider string

const (
	ProviderAnthropic Provider = "anthropic"
	ProviderGemini    Provider = "gemini"
	ProviderOpenAI    Provider = "openai"
	ProviderUnknown   Provider = "unknown"
)

// GuessResult is the result of guessing a signature's provider.
type GuessResult struct {
	Provider Provider
	Reasons  []string
}

// GuessProvider guesses which AI provider a raw base64 signature/reasoning blob belongs to.
//
// Heuristics:
//   - "gAAAA*" prefix → OpenAI (Fernet-like encrypted_content)
//   - "EqQ*" / "Eqo*" / "Eqr*" prefix → Anthropic thinking signature
//   - standard base64 with protobuf-like decoded bytes → Gemini thoughtSignature
//   - otherwise → unknown
func GuessProvider(raw string) GuessResult {
	s := strings.Trim(raw, `"`)

	reasons := make([]string, 0, 3)

	// OpenAI encrypted_content common pattern: gAAAA... (base64url Fernet-like)
	if strings.HasPrefix(s, "gAAAA") || strings.HasPrefix(s, "gAAA") {
		reasons = append(reasons, "starts with gAAA*, commonly seen in OpenAI reasoning.encrypted_content")
		return GuessResult{Provider: ProviderOpenAI, Reasons: reasons}
	}

	// Anthropic thinking signature common prefixes
	if strings.HasPrefix(s, "EqQ") || strings.HasPrefix(s, "Eqo") || strings.HasPrefix(s, "Eqr") {
		reasons = append(reasons, "starts with Eq*, commonly seen in Anthropic thinking.signature")
		return GuessResult{Provider: ProviderAnthropic, Reasons: reasons}
	}

	isStdBase64 := isStdBase64String(s)

	// Standard base64 with protobuf-like decoded bytes → Gemini
	// Valid standard base64 is never OpenAI — OpenAI uses base64url encoding.
	if isStdBase64 {
		decoded, err := base64.StdEncoding.DecodeString(s)
		if err == nil && looksLikeProto(decoded) {
			reasons = append(reasons, "standard base64 with protobuf-like bytes, fits Gemini thoughtSignature")
			return GuessResult{Provider: ProviderGemini, Reasons: reasons}
		}
		reasons = append(reasons, "standard base64 without known provider prefix")
		return GuessResult{Provider: ProviderUnknown, Reasons: reasons}
	}

	// Try base64url decoding (no padding) — could be OpenAI or unknown
	if decoded, err := tryBase64URL(s); err == nil {
		if looksLikeProto(decoded) {
			reasons = append(reasons, "base64url with protobuf-like bytes, possible Gemini")
			return GuessResult{Provider: ProviderGemini, Reasons: reasons}
		}
	}

	reasons = append(reasons, "no known provider heuristics matched")
	return GuessResult{Provider: ProviderUnknown, Reasons: reasons}
}

// IsSafeForProvider checks whether a signature blob is safe to forward to the given provider.
// Returns true only if the blob is recognized as the target provider.
// Returns false for signatures from other providers or unknown formats.
func IsSafeForProvider(raw string, target Provider) bool {
	if raw == "" {
		return false
	}
	result := GuessProvider(raw)
	return result.Provider == target
}

// FilterSignatureForProvider returns the signature if it is safe for the target provider,
// or nil if it clearly belongs to a different provider.
func FilterSignatureForProvider(sig *string, target Provider) *string {
	if sig == nil || *sig == "" {
		return sig
	}
	if IsSafeForProvider(*sig, target) {
		return sig
	}
	return nil
}

// isStdBase64String checks whether s is a valid standard base64 string (with +, /, and = padding).
func isStdBase64String(s string) bool {
	if len(s) == 0 {
		return false
	}
	hasPadding := false
	hasStdChars := false
	for _, c := range s {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			// ok
		case c == '+', c == '/':
			hasStdChars = true
		case c == '=':
			hasPadding = true
		case c == '-', c == '_':
			// base64url chars, not standard base64
			return false
		default:
			return false
		}
	}
	return hasStdChars || hasPadding
}

// tryBase64URL decodes a base64url string (with - and _ instead of + and /).
func tryBase64URL(s string) ([]byte, error) {
	// Add padding if needed
	switch len(s) % 4 {
	case 2:
		s += "=="
	case 3:
		s += "="
	}
	return base64.RawURLEncoding.DecodeString(s)
}

// looksLikeProto does a simple heuristic check: protobuf wire format typically
// starts with a small field number (varint tag byte < 0x80) or a varint length prefix.
func looksLikeProto(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	// First byte: lower 4 bits are field number (1-15 common), upper 3 bits are wire type (0-5)
	b := data[0]
	fieldNum := b >> 3
	wireType := b & 0x07
	return fieldNum >= 1 && fieldNum <= 15 && wireType <= 5
}
