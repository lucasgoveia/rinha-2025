package handlers

import (
	"github.com/bytedance/sonic"
	"net/http"
	"rinha/internal/messages"
	"rinha/internal/payments"
	"sync"
	"time"
)

type PaymentHandler struct {
	publisher *messages.Publisher
}

func NewPaymentHandler(publisher *messages.Publisher) *PaymentHandler {
	return &PaymentHandler{publisher: publisher}
}

var paymentMsgPool = sync.Pool{
	New: func() any {
		return new(payments.PaymentMessage)
	},
}

func (h *PaymentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	msg := paymentMsgPool.Get().(*payments.PaymentMessage)
	*msg = payments.PaymentMessage{}

	err := sonic.ConfigFastest.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		_ = r.Body.Close()
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_ = r.Body.Close()

	msg.RetryCount = 0
	msg.RequestedAt = time.Now().UTC()

	if err := h.publisher.Publish(msg); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		paymentMsgPool.Put(msg)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	paymentMsgPool.Put(msg)
}
