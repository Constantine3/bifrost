package bifrost

import (
	"context"
	"net/http"
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
)

const (
	providerShuttingDownMessage = "provider is shutting down"
	providerShuttingDownType    = schemas.ProviderShuttingDown
)

func assertProviderShuttingDownError(t *testing.T, bifrostErr *schemas.BifrostError) {
	t.Helper()

	if bifrostErr == nil {
		t.Fatal("expected provider shutdown error")
	}
	if bifrostErr.StatusCode == nil {
		t.Fatal("provider shutdown error must include an HTTP status code")
	}
	if got := *bifrostErr.StatusCode; got != http.StatusServiceUnavailable {
		t.Errorf("StatusCode = %d, want %d", got, http.StatusServiceUnavailable)
	}
	if bifrostErr.Type == nil {
		t.Fatal("provider shutdown error must include a top-level type")
	}
	if got := *bifrostErr.Type; got != providerShuttingDownType {
		t.Errorf("Type = %q, want %q", got, providerShuttingDownType)
	}
	if bifrostErr.Error == nil {
		t.Fatal("provider shutdown error must include an Error field")
	}
	if got := bifrostErr.Error.Message; got != providerShuttingDownMessage {
		t.Errorf("Error.Message = %q, want %q", got, providerShuttingDownMessage)
	}
	if bifrostErr.Error.Type == nil {
		t.Fatal("provider shutdown error must include a nested error type")
	}
	if got := *bifrostErr.Error.Type; got != providerShuttingDownType {
		t.Errorf("Error.Type = %q, want %q", got, providerShuttingDownType)
	}
	if bifrostErr.AllowFallbacks != nil {
		t.Errorf("AllowFallbacks = %v, want nil so fallbacks remain enabled", *bifrostErr.AllowFallbacks)
	}
}

func newClosingProviderTestClient(t *testing.T) *Bifrost {
	t.Helper()

	account := NewMockAccount()
	account.AddProvider(schemas.OpenAI, 1, 1)

	client, err := Init(context.Background(), schemas.BifrostConfig{
		Account: account,
		Logger:  NewDefaultLogger(schemas.LogLevelError),
	})
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	t.Cleanup(client.Shutdown)

	queueValue, ok := client.requestQueues.Load(schemas.OpenAI)
	if !ok {
		t.Fatal("OpenAI provider queue was not initialized")
	}
	queueValue.(*ProviderQueue).signalClosing()

	return client
}

func newProviderShutdownChatRequest() *schemas.BifrostChatRequest {
	return &schemas.BifrostChatRequest{
		Provider: schemas.OpenAI,
		Model:    "gpt-4o-mini",
		Input:    []schemas.ChatMessage{{Role: schemas.ChatMessageRoleUser}},
	}
}

func TestProviderShuttingDownErrorsAreRetryable(t *testing.T) {
	t.Run("non-streaming request", func(t *testing.T) {
		client := newClosingProviderTestClient(t)

		_, bifrostErr := client.ChatCompletionRequest(
			schemas.NewBifrostContext(context.Background(), schemas.NoDeadline),
			newProviderShutdownChatRequest(),
		)

		assertProviderShuttingDownError(t, bifrostErr)
	})

	t.Run("streaming request", func(t *testing.T) {
		client := newClosingProviderTestClient(t)

		_, bifrostErr := client.ChatCompletionStreamRequest(
			schemas.NewBifrostContext(context.Background(), schemas.NoDeadline),
			newProviderShutdownChatRequest(),
		)

		assertProviderShuttingDownError(t, bifrostErr)
	})
}

func TestDrainQueueProviderShuttingDownErrorsAreRetryable(t *testing.T) {
	pq := &ProviderQueue{
		queue: make(chan *ChannelMessage, 1),
		done:  make(chan struct{}),
	}
	msg := newTestChannelMessage(schemas.NewBifrostContext(context.Background(), schemas.NoDeadline))
	pq.queue <- msg

	(&Bifrost{}).drainQueueWithErrors(pq)

	select {
	case bifrostErr := <-msg.Err:
		assertProviderShuttingDownError(t, &bifrostErr)
	default:
		t.Fatal("drained request did not receive a provider shutdown error")
	}
}
