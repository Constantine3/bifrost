package integrations

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/maximhq/bifrost/core/schemas"
)

func TestOpenAICompletionWirePreservesReportedTokenCounts(t *testing.T) {
	for _, input := range []string{
		`null`,
		`{"total_tokens":12}`,
		`{"prompt_tokens":0,"total_tokens":12}`,
		`{"completion_tokens":0,"total_tokens":12}`,
		`{"prompt_tokens":12,"total_tokens":12}`,
		`{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}`,
		`{"prompt_tokens":3,"completion_tokens":0,"total_tokens":3}`,
		`{"prompt_tokens":0,"completion_tokens":2,"total_tokens":2}`,
		`{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}`,
	} {
		for _, withCost := range []bool{false, true} {
			var usage *schemas.BifrostLLMUsage
			if err := schemas.Unmarshal([]byte(input), &usage); err != nil {
				t.Fatal(err)
			}
			if usage != nil {
				usage.PromptTokensDetails = &schemas.ChatPromptTokensDetails{CachedReadTokens: 2}
				usage.CompletionTokensDetails = &schemas.ChatCompletionTokensDetails{ReasoningTokens: 1}
				if withCost {
					usage.Cost = &schemas.BifrostCost{TotalCost: 0.25}
				}
			}
			if usage != nil {
				var fields map[string]json.RawMessage
				if err := json.Unmarshal([]byte(input), &fields); err != nil {
					t.Fatal(err)
				}
				usage.PromptTokensExplicitZero = string(fields["prompt_tokens"]) == "0"
				usage.CompletionTokensExplicitZero = string(fields["completion_tokens"]) == "0"
			}
			before, err := schemas.MarshalSorted(usage)
			if err != nil {
				t.Fatal(err)
			}
			for name, response := range map[string]any{
				"chat": &schemas.BifrostChatResponse{ID: "usage-test", Model: "fixture", Usage: usage},
				"text": &schemas.BifrostTextCompletionResponse{ID: "usage-test", Model: "fixture", Usage: usage},
			} {
				t.Run(name+"/"+input, func(t *testing.T) {
					wire, err := schemas.MarshalSorted(openAIWireCostResponse(response))
					if err != nil {
						t.Fatal(err)
					}
					var body struct {
						ID    string         `json:"id"`
						Model string         `json:"model"`
						Usage map[string]any `json:"usage"`
					}
					if err := json.Unmarshal(wire, &body); err != nil {
						t.Fatal(err)
					}
					if body.ID != "usage-test" || body.Model != "fixture" {
						t.Fatalf("response fields lost: %s", wire)
					}
					if usage == nil {
						if body.Usage != nil {
							t.Fatalf("nil usage was fabricated: %s", wire)
						}
						return
					}
					for field, want := range map[string]int{
						"prompt_tokens": usage.PromptTokens, "completion_tokens": usage.CompletionTokens, "total_tokens": usage.TotalTokens,
					} {
						if field == "prompt_tokens" && want == 0 && !usage.PromptTokensExplicitZero || field == "completion_tokens" && want == 0 && !usage.CompletionTokensExplicitZero {
							if _, exists := body.Usage[field]; exists {
								t.Errorf("unreported %s was added: %s", field, wire)
							}
							continue
						}
						if got, ok := body.Usage[field].(float64); !ok || got != float64(want) {
							t.Errorf("required count %s lost: %s", field, wire)
						}
					}
					for _, field := range []string{"prompt_tokens_details", "completion_tokens_details"} {
						if _, ok := body.Usage[field].(map[string]any); !ok {
							t.Errorf("usage details %s lost: %s", field, wire)
						}
					}
					if withCost && body.Usage["cost"] != 0.25 {
						t.Errorf("cost must remain a bare total: %s", wire)
					}
					if !withCost {
						if _, ok := body.Usage["cost"]; ok {
							t.Errorf("cost was fabricated: %s", wire)
						}
					}
				})
			}
			after, err := schemas.MarshalSorted(usage)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatalf("shared usage was mutated: before %s, after %s", before, after)
			}
		}
	}
}

// TestOpenAIWireCostResponse verifies 2A: the OpenAI-compatible wire renders
// usage.cost as the bare total (float), the legacy shape SDKs accept, instead of
// the nested BifrostCost object. The rewrite wraps rather than mutates, so the
// shared response is left intact.
func TestOpenAIWireCostResponse(t *testing.T) {
	t.Run("chat renders cost as float total without mutating the shared response", func(t *testing.T) {
		cost := &schemas.BifrostCost{InputCost: 1, OutputCost: 2, TotalCost: 3}
		usage := &schemas.BifrostLLMUsage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15, Cost: cost}
		resp := &schemas.BifrostChatResponse{Usage: usage}

		out := openAIWireCostResponse(resp)
		if any(out) == any(resp) {
			t.Fatal("expected a wire wrapper, got the shared response")
		}
		js, err := sonic.Marshal(out)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		s := string(js)
		if !strings.Contains(s, `"cost":3`) {
			t.Errorf("wire cost is not the bare total float: %s", s)
		}
		if strings.Contains(s, "total_cost") || strings.Contains(s, "input_cost") {
			t.Errorf("wire still carries the nested cost object: %s", s)
		}
		if !strings.Contains(s, `"total_tokens":15`) {
			t.Errorf("usage fields lost in wrapper: %s", s)
		}

		// The shared response keeps its full breakdown object.
		if usage.Cost != cost {
			t.Error("shared response usage.cost was mutated")
		}
	})

	t.Run("responses renders cost as float total", func(t *testing.T) {
		resp := &schemas.BifrostResponsesResponse{
			Usage: &schemas.ResponsesResponseUsage{TotalTokens: 15, Cost: &schemas.BifrostCost{TotalCost: 3}},
		}
		js, err := sonic.Marshal(openAIWireCostResponse(resp))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if s := string(js); !strings.Contains(s, `"cost":3`) || strings.Contains(s, "total_cost") {
			t.Errorf("responses wire cost not flattened to float: %s", s)
		}
		if resp.Usage.Cost == nil {
			t.Error("shared responses usage.cost was mutated")
		}
	})

	t.Run("responses keeps tool_usage top-level beside the flattened cost", func(t *testing.T) {
		resp := &schemas.BifrostResponsesResponse{
			Usage: &schemas.ResponsesResponseUsage{
				TotalTokens: 15,
				Cost:        &schemas.BifrostCost{TotalCost: 3},
				ToolUsage:   &schemas.ToolUsage{WebSearch: &schemas.WebSearchToolUsage{NumRequests: 2}},
			},
		}
		js, err := sonic.Marshal(openAIWireCostResponse(resp))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var wire struct {
			ToolUsage *schemas.ToolUsage     `json:"tool_usage"`
			Usage     map[string]interface{} `json:"usage"`
		}
		if err := sonic.Unmarshal(js, &wire); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if wire.ToolUsage == nil || wire.ToolUsage.WebSearch == nil || wire.ToolUsage.WebSearch.NumRequests != 2 {
			t.Errorf("top-level tool_usage missing: %s", js)
		}
		if _, ok := wire.Usage["tool_usage"]; ok {
			t.Errorf("usage must not carry tool_usage: %s", js)
		}
		if wire.Usage["cost"] != float64(3) {
			t.Errorf("responses wire cost not flattened to float: %s", js)
		}
	})

	t.Run("image renders cost as float total", func(t *testing.T) {
		resp := &schemas.BifrostImageGenerationResponse{
			Usage: &schemas.ImageUsage{Cost: &schemas.BifrostCost{TotalCost: 3}},
		}
		js, err := sonic.Marshal(openAIWireCostResponse(resp))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if s := string(js); !strings.Contains(s, `"cost":3`) || strings.Contains(s, "total_cost") {
			t.Errorf("image wire cost not flattened to float: %s", s)
		}
		if resp.Usage.Cost == nil {
			t.Error("shared image usage.cost was mutated")
		}
	})

	t.Run("chat without cost and with nonzero counts passes through the same pointer", func(t *testing.T) {
		resp := &schemas.BifrostChatResponse{Usage: &schemas.BifrostLLMUsage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15}}
		if out := openAIWireCostResponse(resp); any(out) != any(resp) {
			t.Error("cost-free response should pass through unchanged")
		}
	})

	t.Run("raw upstream payload passes through unchanged", func(t *testing.T) {
		raw := "raw-openai-json"
		if out := openAIWireCostResponse(raw); out != raw {
			t.Errorf("raw passthrough altered: %v", out)
		}
	})
}
