package payments

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"net/http"
)

type ServiceType string

const (
	ServiceTypeDefault  ServiceType = "default"
	ServiceTypeFallback ServiceType = "fallback"
)

var (
	ErrUnavailableProcessor = errors.New("unavailable processor")
)

type PaymentService struct {
	fee         float64
	serviceURL  string
	serviceType ServiceType
	httpClient  *http.Client
	dbpool      *pgxpool.Pool
}

func NewPaymentService(httpClient *http.Client, fee float64, serviceURL string, serviceType ServiceType, dbpool *pgxpool.Pool) *PaymentService {
	return &PaymentService{
		httpClient:  httpClient,
		fee:         fee,
		serviceURL:  serviceURL,
		serviceType: serviceType,
		dbpool:      dbpool,
	}
}

func (s *PaymentService) Process(ctx context.Context, msg PaymentMessage) error {
	return s.callPaymentService(ctx, msg)
}

func (s *PaymentService) callPaymentService(ctx context.Context, payload PaymentMessage) error {
	// Create a span for the HTTP call
	tracer := otel.Tracer("payment-service")
	ctx, span := tracer.Start(ctx, "call-payment-service", trace.WithAttributes(
		attribute.String("service.url", s.serviceURL),
		attribute.String("service.type", string(s.serviceType)),
		attribute.Float64("payment.Amount", payload.Amount),
		attribute.String("payment.correlation_id", payload.CorrelationId),
	))
	defer span.End()

	bodyJSON, err := json.Marshal(payload)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to serialize request body")
		return fmt.Errorf("failed to serialize the request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.serviceURL, bytes.NewReader(bodyJSON))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create HTTP request")
		return fmt.Errorf("unable to create http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	span.AddEvent("sending-http-request")
	resp, err := s.httpClient.Do(req)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Error sending HTTP request")
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ErrUnavailableProcessor
	}

	if resp != nil {
		span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))
	}

	if resp != nil && resp.StatusCode == 422 {
		return nil
	}

	if resp != nil && (resp.StatusCode >= 500 || resp.StatusCode == 429 || resp.StatusCode == 408) {
		return ErrUnavailableProcessor
	}

	span.SetStatus(codes.Ok, "Payment service call successful")
	return nil
}
