package handlers

import (
	"github.com/bytedance/sonic"
	"net/http"
	"rinha/internal/payments"
	"rinha/internal/payments/workers"
	"time"
)

type PaymentHandler struct {
	workerPool *workers.WorkerPool
}

type paymentRequest struct {
	Amount        float64 `json:"amount"`
	CorrelationId string  `json:"correlationId"`
}

func NewPaymentHandler(workerPool *workers.WorkerPool) *PaymentHandler {
	return &PaymentHandler{workerPool: workerPool}
}

func (h *PaymentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req paymentRequest

	err := sonic.ConfigFastest.NewDecoder(r.Body).Decode(&req)
	defer r.Body.Close()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	msg := payments.PaymentMessage{
		Amount:        req.Amount,
		CorrelationId: req.CorrelationId,
		RetryCount:    0,
		RequestedAt:   time.Now().UTC(),
	}

	ok := h.workerPool.Submit(&msg)
	if !ok {
		w.WriteHeader(http.StatusTooManyRequests) // 429
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
