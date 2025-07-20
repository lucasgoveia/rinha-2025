package handlers

import (
	"encoding/json"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"rinha/internal/payments"
	"time"
)

type PaymentHandler struct {
	redisClient *redis.Client
}

type paymentRequest struct {
	Amount        float64 `json:"amount"`
	CorrelationId string  `json:"correlationId"`
}

func NewPaymentHandler(redisClient *redis.Client) *PaymentHandler {
	return &PaymentHandler{
		redisClient: redisClient,
	}
}

func (h *PaymentHandler) Handle(c echo.Context) error {
	ctx := c.Request().Context()
	tracer := otel.Tracer("payment-handler")
	ctx, span := tracer.Start(ctx, "payment-handler", trace.WithAttributes(
		attribute.String("handler", "payment"),
	))
	defer span.End()

	var req paymentRequest
	if err := c.Bind(&req); err != nil {
		span.RecordError(err)
		c.Error(err)
		return c.NoContent(400)
	}

	paymentMsg := payments.PaymentMessage{
		Amount:        req.Amount,
		CorrelationId: req.CorrelationId,
		RequestedAt:   time.Now().UTC(),
		RetryCount:    0,
	}

	span.SetAttributes(
		attribute.Float64("payment.amount", req.Amount),
		attribute.String("payment.correlation_id", req.CorrelationId),
	)

	data, err := json.Marshal(paymentMsg)
	if err != nil {
		span.RecordError(err)
		c.Logger().Errorf("error while marshalling the payment: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	start := time.Now()
	_, err = h.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "payments",
		Values: map[string]interface{}{
			"data": string(data),
		},
	}).Result()
	c.Logger().Info("XADD levou %v", time.Since(start))

	if err != nil {
		span.RecordError(err)
		c.Logger().Errorf("error while publishing the payment: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}
	
	return c.NoContent(http.StatusAccepted)
}
