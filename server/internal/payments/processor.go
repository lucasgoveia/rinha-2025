package payments

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"io"
	"log/slog"
	"net/http"
	"sync"
)

type ProcessorType string

const (
	ProcessorTypeDefault  ProcessorType = "default"
	ProcessorTypeFallback ProcessorType = "fallback"
)

var (
	ErrUnavailableProcessor = errors.New("unavailable processor")
	ErrInvalidPayment       = errors.New("invalid payment")
)

type PaymentProcessor struct {
	processorURL  string
	processorType ProcessorType
	httpClient    *http.Client
	logger        *slog.Logger
}

func NewPaymentProcessor(httpClient *http.Client, serviceURL string, serviceType ProcessorType, logger *slog.Logger) *PaymentProcessor {
	return &PaymentProcessor{
		httpClient:    httpClient,
		processorURL:  serviceURL,
		processorType: serviceType,
		logger:        logger,
	}
}

var bufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 512))
	},
}

func (s *PaymentProcessor) Process(ctx context.Context, msg *PaymentMessage) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()

	if err := sonic.ConfigFastest.NewEncoder(buf).Encode(msg); err != nil {
		bufPool.Put(buf)
		return fmt.Errorf("failed to serialize request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.processorURL, io.NopCloser(buf))
	if err != nil {
		bufPool.Put(buf)
		return fmt.Errorf("unable to create http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)

	bufPool.Put(buf)
	if resp != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		s.logger.Warn("payment processing request timed out", "processor", s.processorType, "url", s.processorURL, "error", err)
		return ErrUnavailableProcessor
	}

	if resp != nil && resp.StatusCode == 422 {
		return ErrInvalidPayment
	}

	if resp != nil && (resp.StatusCode >= 500 || resp.StatusCode == 429 || resp.StatusCode == 408) {
		return ErrUnavailableProcessor
	}

	return nil
}
