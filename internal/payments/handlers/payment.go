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

func NewPaymentHandler(workerPool *workers.WorkerPool) *PaymentHandler {
	return &PaymentHandler{workerPool: workerPool}
}

func (h *PaymentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var msg payments.PaymentMessage

	err := sonic.ConfigFastest.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		_ = r.Body.Close()
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_ = r.Body.Close()

	msg.RetryCount = 0
	msg.RequestedAt = time.Now().UTC()

	ok := h.workerPool.Submit(&msg)
	if !ok {
		w.WriteHeader(http.StatusTooManyRequests) // 429
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
