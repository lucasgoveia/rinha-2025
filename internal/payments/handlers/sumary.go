package handlers

import (
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"net/http"
	"time"
)

type GetPaymentsSumaryHandler struct {
	dbpool *pgxpool.Pool
}

func NewGetPaymentsSumaryHandler(dbpool *pgxpool.Pool) *GetPaymentsSumaryHandler {
	return &GetPaymentsSumaryHandler{
		dbpool: dbpool,
	}
}

type serviceSumary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type sumaryResponse struct {
	Default  serviceSumary `json:"default"`
	Fallback serviceSumary `json:"fallback"`
}

const query = `
	SELECT COUNT(*) AS total_requests,
	      SUM(amount) AS total_amount,
	      service_used
	FROM payments
	WHERE ($1::timestamp IS NULL OR requested_at >= $1::timestamp)
	 AND ($2::timestamp IS NULL OR requested_at <= $2::timestamp)
	GROUP BY service_used;
	`

func (h *GetPaymentsSumaryHandler) Handle(c echo.Context) error {
	fromStr := c.QueryParam("from")
	toStr := c.QueryParam("to")

	var fromParam, toParam = pgtype.Timestamp{Valid: false}, pgtype.Timestamp{Valid: false}
	if fromStr != "" {
		parsed, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			c.Logger().Errorf("Error parsing 'from' date: %v", err)
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid 'from' date format")
		}
		fromParam = pgtype.Timestamp{
			Valid: true,
			Time:  parsed,
		}
	}

	if toStr != "" {
		parsed, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			c.Logger().Errorf("Error parsing 'to' date: %v", err)
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid 'to' date format")
		}
		toParam = pgtype.Timestamp{
			Valid: true,
			Time:  parsed,
		}
	}

	ctx := c.Request().Context()

	var defaultsummary serviceSumary
	var fallbacksummary serviceSumary

	rows, err := h.dbpool.Query(ctx, query, fromParam, toParam)
	if err != nil {
		c.Logger().Errorf("Error while querying payments: %v", err)
		return err
	}

	for rows.Next() {
		var totalRequests int
		var totalAmount float64
		var processor string
		_ = rows.Scan(&totalRequests, &totalAmount, &processor)

		if processor == "default" {
			defaultsummary.TotalRequests = totalRequests
			defaultsummary.TotalAmount = totalAmount
		} else {
			fallbacksummary.TotalRequests = totalRequests
			fallbacksummary.TotalAmount = totalAmount
		}
	}

	return c.JSON(200, sumaryResponse{
		Default:  defaultsummary,
		Fallback: fallbacksummary,
	})
}
