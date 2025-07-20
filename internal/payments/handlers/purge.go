package handlers

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"net/http"
)

type PurgePaymentsHandler struct {
	dbpool *pgxpool.Pool
}

func NewPurgePaymentsHandler(dbpool *pgxpool.Pool) *PurgePaymentsHandler {
	return &PurgePaymentsHandler{
		dbpool: dbpool,
	}
}

func (h *PurgePaymentsHandler) Handle(c echo.Context) error {
	ctx := c.Request().Context()
	tracer := otel.Tracer("purge-payments-handler")
	ctx, span := tracer.Start(ctx, "purge-payments-handler", trace.WithAttributes(
		attribute.String("handler", "purge-payments"),
	))
	defer span.End()

	_, err := h.dbpool.Exec(ctx, "TRUNCATE TABLE payments")
	if err != nil {
		span.RecordError(err)
		c.Logger().Errorf("Error purging payments: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.NoContent(http.StatusOK)
}
